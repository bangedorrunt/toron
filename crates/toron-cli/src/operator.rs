// governed-by: ADR-0003
// governed-by: ADR-0004 (run_doctor_reconstruct — Ship 3)

//! Operator surface (bd-led.8.2): doctor/metrics/key/project/relay/backend.
//!
//! The daemon-persona commands. Read ops emit JSON on stdout (clig.dev:
//! stdout = data, stderr = messages). Exit-code discipline: 0 ok · 1 runtime ·
//! 2 usage (clap) · 3 doctor `--check` fails-closed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde_json::json;

use crate::cli::{
    AgentCommand, BackendCommand, Command, ContactCommand, DoctorArgs, DoctorCmd,
    DoctorReconstructArgs, GuardCommand, KeyCommand, KeyGenerateArgs, KeyInspectArgs, MailCommand,
    ProjectCommand, ProjectEnsureArgs, RelayCommand, RelayUrlArg, ReserveCommand, SlotCommand,
};
use crate::config::Config;
use crate::output::{OutputFormat, render_json};
use toron_mcp::ToJson;

/// scrypt log-n for operator keys (P3 bd-p3-argon2-kdf-dos-bound-7sbu:
/// raised 12 -> 14 — matches toron-core OPERATOR_KEY_LOG_N; 12 was 16x
/// weaker than agent keys (NIP-49 log_n=16). Decrypt reads log_n from the
/// payload, so existing keys keep working).
const KEY_LOG_N: u8 = 14;

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

/// bd-legacy-slug-inbox-onoj: list slug identities whose registry fold
/// points elsewhere — standalone registrations a fold never absorbed (a
/// second identity beside the pin; mail sealed to its npub is unreachable).
/// Informational (ok stays true): doctor NAMES the rows, cleanup is the
/// fold path's job (`fold_agent_name_alias` absorbs idempotently).
fn check_legacy_slug_identities(cfg: &Config) -> CheckResult {
    let name = "legacy_slug_identities";
    let Some(data_dir) = resolve_data_dir(cfg, None) else {
        return CheckResult {
            name,
            ok: true,
            detail: "no data dir — nothing to scan".into(),
        };
    };
    let db_path = data_dir.join("index.sqlite");
    if !db_path.exists() {
        return CheckResult {
            name,
            ok: true,
            detail: format!("no index at {}", db_path.display()),
        };
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            return CheckResult {
                name,
                ok: false,
                detail: format!("runtime build failed: {e}"),
            };
        }
    };
    let scanned = rt.block_on(async {
        let pool = toron_core::index::schema::connect(db_path.to_str().unwrap()).await?;
        let leftovers = toron_core::mailbox::identity::folded_slug_leftovers(&pool).await?;
        pool.close().await;
        Ok::<_, anyhow::Error>(leftovers)
    });
    match scanned {
        Ok(leftovers) if leftovers.is_empty() => CheckResult {
            name,
            ok: true,
            detail: "no folded-slug dual registrations".into(),
        },
        Ok(leftovers) => {
            let rows: Vec<String> = leftovers
                .iter()
                .map(|l| {
                    format!(
                        "{}:{} → {} (npub {})",
                        l.project_slug, l.slug, l.canonical, l.pubkey
                    )
                })
                .collect();
            CheckResult {
                name,
                ok: true,
                detail: format!(
                    "{} leftover legacy registration(s): {}",
                    leftovers.len(),
                    rows.join("; ")
                ),
            }
        }
        Err(e) => CheckResult {
            name,
            ok: false,
            detail: format!("scan failed: {e:#}"),
        },
    }
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

/// Parse the HTTP status code from a response head. Requires a real status
/// line (`HTTP/x.y NNN …`); rejects incidental `"200"` substrings and bare
/// non-status bodies. Returns `None` when the body is not HTTP.
fn http_response_status(head: &str) -> Option<u16> {
    let line = head.lines().next()?.trim();
    let mut parts = line.split_whitespace();
    let proto = parts.next()?;
    if !proto.starts_with("HTTP/") {
        return None;
    }
    parts.next()?.parse().ok()
}

fn check_resonate() -> CheckResult {
    // GS-037 P1: probe 127.0.0.1:8001 (IPv4 — resonate binds v4; localhost
    // may resolve ::1-first). Fail-closed unless TCP opens AND /health
    // returns HTTP 2xx — no invent-green on any HTTP/ line, incidental 200
    // substring, or non-HTTP junk on an open port.
    let addr: std::net::SocketAddr = "127.0.0.1:8001"
        .parse()
        .expect("static resonate probe addr");
    check_resonate_at(addr)
}

fn check_resonate_at(addr: std::net::SocketAddr) -> CheckResult {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;
    match TcpStream::connect_timeout(&addr, Duration::from_secs(1)) {
        Ok(mut s) => {
            let _ = s.set_read_timeout(Some(Duration::from_millis(500)));
            let _ = s.set_write_timeout(Some(Duration::from_millis(500)));
            let host = match addr {
                std::net::SocketAddr::V4(a) => a.ip().to_string(),
                std::net::SocketAddr::V6(a) => a.ip().to_string(),
            };
            let req = format!("GET /health HTTP/1.0\r\nHost: {host}\r\nConnection: close\r\n\r\n");
            let _ = s.write_all(req.as_bytes());
            let mut buf = [0u8; 256];
            let n = s.read(&mut buf).unwrap_or(0);
            let head = String::from_utf8_lossy(&buf[..n]);
            match http_response_status(&head) {
                Some(code) if (200..300).contains(&code) => CheckResult {
                    name: "resonate",
                    ok: true,
                    detail: format!("reachable at http://{addr}"),
                },
                Some(code) => CheckResult {
                    name: "resonate",
                    ok: false,
                    detail: format!(
                        "port open at {addr} but /health returned HTTP {code} (need 2xx) — refuse soft-ok invent-green (GS-037)"
                    ),
                },
                None => CheckResult {
                    name: "resonate",
                    ok: false,
                    detail: format!(
                        "port open at {addr} but health body is not HTTP 2xx — refuse soft-ok invent-green (GS-037)"
                    ),
                },
            }
        }
        Err(e) => CheckResult {
            name: "resonate",
            // GS-037: fail-closed on unreachable — doctor --check exits 3
            // when resonate is down (medium invent-green / soft-ok fix).
            ok: false,
            detail: format!(
                "not reachable at http://{addr} ({e}) — refuse soft-ok invent-green (GS-037); start resonate"
            ),
        },
    }
}

/// Deep health checks. `--check` exits 3 when any check fails (fails-closed,
/// CI-friendly); 0 when all pass. Runtime failures still exit 1.
pub fn run_doctor(
    cfg: &Config,
    args: &DoctorArgs,
    format: OutputFormat,
) -> anyhow::Result<ExitCode> {
    // Disaster-recovery subcommands take precedence over the check surface.
    if let Some(DoctorCmd::Reconstruct(rargs)) = &args.command {
        return run_doctor_reconstruct(cfg, rargs, format);
    }
    let checks = vec![
        check_herdr(),
        check_git(),
        check_keys(cfg),
        check_resonate(),
        check_legacy_slug_identities(cfg),
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
// doctor reconstruct (bd-jwa, ADR-0004 Ship 3): rebuild the SQLite index
// from the Git archive. AM `am doctor reconstruct` parity. Reuses the proven
// `rebuild_index_from_git` path (proof: sc14a_dr_rebuild test).
//
// Exit codes: 0 ok · 1 rebuild failed (quarantine preserved) · 3 diagnostic
// (nothing to reconstruct / cannot locate data dir).
// ---------------------------------------------------------------------------

/// One project's reconstruct outcome.
#[derive(Debug, Clone, serde::Serialize)]
struct ReconstructReport {
    slug: String,
    archive_messages: usize,
    index_rows_before: Option<i64>,
    rebuilt_rows: Option<usize>,
    /// Path to the forensic bundle (original db + manifest) when a rebuild
    /// ran. `None` for healthy/dry-run-skip.
    quarantine: Option<String>,
    /// "healthy" | "rebuilt" | "would_rebuild" | "failed".
    status: String,
    reason: Option<String>,
}

/// Resolve the data directory: `--data-dir` > `TORON_DATA_DIR` > config
/// `data_dir` > the durable XDG root (`$XDG_DATA_HOME/toron` /
/// `~/.local/share/toron`, ADR-0007 D3). Never a temp per-pid root — the
/// default is always Some.
fn resolve_data_dir(cfg: &Config, override_dir: Option<&Path>) -> Option<PathBuf> {
    override_dir
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("TORON_DATA_DIR").map(PathBuf::from))
        .or_else(|| cfg.data_dir.clone())
        .or_else(|| Some(toron_core::config::durable_data_dir()))
}

/// Enumerate project roots under `data_dir`: subdirs that contain a
/// `messages/` archive (the Git archive signature). If `filter` is set,
/// restrict to that slug.
fn enumerate_projects(data_dir: &Path, filter: Option<&str>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let slug = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some(f) = filter
            && f != slug
        {
            continue;
        }
        // Archive signature: a `messages/` dir (the rebuild path reads it).
        if path.join("messages").is_dir() {
            out.push(path);
        }
    }
    out
}

/// Count archived messages (`messages/**/*.json`) — mirrors the rebuild walk
/// so the pre-check count is directly comparable.
fn count_archive_messages(archive_root: &Path) -> usize {
    let messages_root = archive_root.join("messages");
    let mut count = 0;
    let Ok(years) = std::fs::read_dir(&messages_root) else {
        return 0;
    };
    for year in years.flatten() {
        if !year.path().is_dir() {
            continue;
        }
        let Ok(months) = std::fs::read_dir(year.path()) else {
            continue;
        };
        for month in months.flatten() {
            if !month.path().is_dir() {
                continue;
            }
            let Ok(files) = std::fs::read_dir(month.path()) else {
                continue;
            };
            for f in files.flatten() {
                if f.path().extension().map(|e| e == "json") == Some(true) {
                    count += 1;
                }
            }
        }
    }
    count
}

/// Inspect an existing index via the core helper (no raw sqlx in the CLI).
async fn inspect_index(db_path: &Path) -> anyhow::Result<(bool, bool, Option<i64>)> {
    toron_core::index::rebuild::inspect_index(db_path).await
}

/// Seconds since UNIX epoch as a decimal string (quarantine suffix).
fn unix_ts_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Reconstruct one project root (async inner; wrapped in block_on by the
/// sync entry point).
async fn reconstruct_project(root: &Path, dry_run: bool) -> anyhow::Result<ReconstructReport> {
    let slug = root
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let db_path = root.join("index.sqlite");
    let archive_messages = count_archive_messages(root);

    // Current state of the index (if any).
    let (db_exists, integrity_ok, index_rows) = inspect_index(&db_path).await?;

    // Healthy: present, integral, rows match the archive. Skip (no mutation).
    if db_exists && integrity_ok && index_rows == Some(archive_messages as i64) {
        return Ok(ReconstructReport {
            slug,
            archive_messages,
            index_rows_before: index_rows,
            rebuilt_rows: None,
            quarantine: None,
            status: "healthy".into(),
            reason: None,
        });
    }

    // Dry-run: report what would happen, mutate nothing.
    if dry_run {
        return Ok(ReconstructReport {
            slug,
            archive_messages,
            index_rows_before: index_rows,
            rebuilt_rows: None,
            quarantine: None,
            status: "would_rebuild".into(),
            reason: Some(reason_needs_rebuild(
                db_exists,
                integrity_ok,
                index_rows,
                archive_messages,
            )),
        });
    }

    // Forensic bundle + quarantine the original db (and its wal/shm).
    let ts = unix_ts_secs();
    let forensic_dir = root.join(format!("doctor-forensic-{ts}"));
    let mut quarantine = None;
    if db_exists {
        std::fs::create_dir_all(&forensic_dir)?;
        for suffix in ["", "-wal", "-shm"] {
            let src = root.join(format!("index.sqlite{suffix}"));
            if src.exists() {
                let dest = forensic_dir.join(format!("index.sqlite{suffix}.orig"));
                std::fs::rename(&src, &dest)?;
            }
        }
        let manifest = json!({
            "slug": slug,
            "ts": ts,
            "archive_messages": archive_messages,
            "index_rows_before": index_rows,
            "action": "reconstruct",
        });
        std::fs::write(
            forensic_dir.join("manifest.json"),
            serde_json::to_string_pretty(&manifest)?,
        )?;
        quarantine = Some(forensic_dir.to_string_lossy().into_owned());
    }

    // Build a fresh db at `index.sqlite.new`, migrate, rebuild from archive.
    let new_db = root.join("index.sqlite.new");
    let _ = std::fs::remove_file(&new_db); // clear any stale .new
    let new_db_str = new_db.to_string_lossy().to_string();
    let (rebuilt, rows) =
        match toron_core::index::rebuild::build_fresh_index(&new_db_str, root).await {
            Ok(pair) => pair,
            Err(e) => {
                let _ = std::fs::remove_file(&new_db);
                return Ok(ReconstructReport {
                    slug,
                    archive_messages,
                    index_rows_before: index_rows,
                    rebuilt_rows: None,
                    quarantine,
                    status: "failed".into(),
                    reason: Some(format!("rebuild failed: {e}")),
                });
            }
        };

    // Convergence verify: fresh rows == rebuilt == archive count.
    if rows != rebuilt || rebuilt != archive_messages {
        let _ = std::fs::remove_file(&new_db);
        return Ok(ReconstructReport {
            slug,
            archive_messages,
            index_rows_before: index_rows,
            rebuilt_rows: Some(rebuilt),
            quarantine,
            status: "failed".into(),
            reason: Some(format!(
                "verify mismatch: archive={archive_messages} rebuilt={rebuilt} rows={rows}"
            )),
        });
    }

    // Atomic swap: `index.sqlite.new` → `index.sqlite` (POSIX atomic rename).
    // Drop the now-empty WAL siblings first so the renamed main file is
    // self-contained (no orphaned -wal/-shm).
    let _ = std::fs::remove_file(root.join("index.sqlite.new-wal"));
    let _ = std::fs::remove_file(root.join("index.sqlite.new-shm"));
    std::fs::rename(&new_db, &db_path)?;
    Ok(ReconstructReport {
        slug,
        archive_messages,
        index_rows_before: index_rows,
        rebuilt_rows: Some(rebuilt),
        quarantine,
        status: "rebuilt".into(),
        reason: None,
    })
}

/// Human-readable reason for why a rebuild is needed (dry-run explanation).
fn reason_needs_rebuild(
    db_exists: bool,
    integrity_ok: bool,
    index_rows: Option<i64>,
    archive_messages: usize,
) -> String {
    if !db_exists {
        format!("index missing; {archive_messages} archived messages to restore")
    } else if !integrity_ok {
        "integrity_check failed".into()
    } else if let Some(rows) = index_rows {
        format!("row drift: index {rows} vs archive {archive_messages}")
    } else {
        format!("messages table unreadable; {archive_messages} archived")
    }
}

/// Reconstruct outcome (testable; mapped to an exit code at the edge).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconstructExit {
    /// All projects healthy or rebuilt successfully.
    Ok,
    /// At least one project failed to rebuild (quarantine preserved).
    Failed,
    /// Nothing to do: data dir missing/unset, or no project archives found.
    Diagnostic,
}

/// Pure decision over the reconstruct results (ADR-0004 Ship 3 exit-code
/// discipline: 0 ok · 1 failed · 3 diagnostic).
fn classify_exit(
    reports: &[ReconstructReport],
    resolved: bool,
    project_count: usize,
) -> ReconstructExit {
    if !resolved || project_count == 0 {
        return ReconstructExit::Diagnostic;
    }
    if reports.iter().any(|r| r.status == "failed") {
        return ReconstructExit::Failed;
    }
    ReconstructExit::Ok
}

/// `toron doctor reconstruct` entry point (sync; spawns a runtime for the
/// async sqlx/rebuild work).
pub fn run_doctor_reconstruct(
    cfg: &Config,
    args: &DoctorReconstructArgs,
    format: OutputFormat,
) -> anyhow::Result<ExitCode> {
    let data_dir = resolve_data_dir(cfg, args.data_dir.as_deref());
    let resolved = data_dir.as_ref().map(|d| d.is_dir()).unwrap_or(false);
    let projects = if resolved {
        enumerate_projects(data_dir.as_ref().unwrap(), args.project.as_deref())
    } else {
        Vec::new()
    };

    // Run the reconstruct per project under a dedicated runtime (run_doctor
    // is sync; the work is async sqlx/rebuild).
    let mut reports = Vec::with_capacity(projects.len());
    if resolved && !projects.is_empty() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        for root in &projects {
            reports.push(rt.block_on(reconstruct_project(root, args.dry_run))?);
        }
    }

    let outcome = classify_exit(&reports, resolved, projects.len());

    match format {
        OutputFormat::Json | OutputFormat::Plain => {
            let value = if resolved && !projects.is_empty() {
                json!({ "dry_run": args.dry_run, "reports": serde_json::to_value(&reports)? })
            } else {
                json!({
                    "status": "diagnostic",
                    "data_dir": data_dir.as_ref().map(|d| d.display().to_string()),
                    "error": if !resolved {
                        "no data dir (pass --data-dir, set TORON_DATA_DIR, or configure data_dir)".to_string()
                    } else {
                        "no project archives found (no subdirs with a messages/ dir)".to_string()
                    },
                })
            };
            render_json(&value, format);
        }
        OutputFormat::Human => {
            if !resolved || projects.is_empty() {
                let detail = if !resolved {
                    "no data dir located".to_string()
                } else {
                    format!(
                        "no project archives under {}",
                        data_dir.as_ref().unwrap().display()
                    )
                };
                println!("toron doctor reconstruct — diagnostic: {detail}");
            } else {
                println!(
                    "toron doctor reconstruct{}",
                    if args.dry_run { " (dry-run)" } else { "" }
                );
                for r in &reports {
                    let detail = match r.status.as_str() {
                        "healthy" => format!(
                            "{} archived, {} rows — no action",
                            r.archive_messages,
                            r.index_rows_before.unwrap_or(-1)
                        ),
                        "would_rebuild" => format!(
                            "would rebuild {} archived ({})",
                            r.archive_messages,
                            r.reason.as_deref().unwrap_or("")
                        ),
                        "rebuilt" => format!(
                            "rebuilt {} rows from {} archived",
                            r.rebuilt_rows.unwrap_or(0),
                            r.archive_messages
                        ),
                        "failed" => format!("FAILED: {}", r.reason.as_deref().unwrap_or("unknown")),
                        _ => r.status.clone(),
                    };
                    let q = r
                        .quarantine
                        .as_deref()
                        .map(|p| format!("  forensic: {p}"))
                        .unwrap_or_default();
                    println!("  [{:>12}] {} — {}{}", r.status, r.slug, detail, q);
                }
            }
        }
    }

    Ok(match outcome {
        ReconstructExit::Ok => ExitCode::SUCCESS,
        ReconstructExit::Failed => ExitCode::from(1),
        ReconstructExit::Diagnostic => ExitCode::from(3),
    })
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
        if let Ok(raw) = std::fs::read_to_string(metrics_file_path())
            && let Ok(parsed) = serde_json::from_str::<MetricsState>(&raw)
        {
            loaded = parsed;
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
        Command::Mail(MailCommand::Whois(_)) => "mail.whois",
        Command::AckSla(_) => "mail.ack_sla",
        Command::Reserve(ReserveCommand::Acquire(_)) => "reserve.acquire",
        Command::Reserve(ReserveCommand::Conflicts(_)) => "reserve.conflicts",
        Command::Reserve(ReserveCommand::Renew(_)) => "reserve.renew",
        Command::Reserve(ReserveCommand::Release(_)) => "reserve.release",
        Command::Reserve(ReserveCommand::ForceRelease(_)) => "reserve.force_release",
        Command::Reserve(ReserveCommand::RequestRelease(_)) => "reserve.request_release",
        Command::Reserve(ReserveCommand::Respond(_)) => "reserve.respond",
        Command::Reserve(ReserveCommand::Transfer(_)) => "reserve.transfer",
        Command::Slot(SlotCommand::Acquire(_)) => "slot.acquire",
        Command::Slot(SlotCommand::Renew(_)) => "slot.renew",
        Command::Slot(SlotCommand::Release(_)) => "slot.release",
        Command::Contact(ContactCommand::Request(_)) => "contact.request",
        Command::Contact(ContactCommand::Respond(_)) => "contact.respond",
        Command::Contact(ContactCommand::List(_)) => "contact.list",
        Command::Contact(ContactCommand::Policy(_)) => "contact.policy",
        Command::Contact(ContactCommand::Captain(_)) => "contact.captain",
        Command::Guard(GuardCommand::Install(_)) => "guard.install",
        Command::Guard(GuardCommand::Uninstall(_)) => "guard.uninstall",
        Command::Agent(AgentCommand::Bootstrap(_)) => "agent.bootstrap",
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
    // R2 (ADR-0007 D3): list projects under the resolved durable data dir —
    // the `/tmp/toron-mcp-demo` hardcode is dead as the default.
    let cfg = Config::load(None);
    let Some(data_dir) = resolve_data_dir(&cfg, None) else {
        anyhow::bail!("no data dir");
    };
    let mut projects: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                projects.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    projects.sort();
    let note = if data_dir.is_dir() {
        format!("local listing of data dir ({})", data_dir.display())
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

#[cfg(test)]
mod check_resonate_tests {
    //! GS-037: doctor resonate must not invent-green on soft-ok health.
    use super::*;
    use std::io::Write;
    use std::net::TcpListener;
    use std::thread;
    use std::time::Duration;

    /// Accept one connection and write `reply` (or drop without writing).
    fn serve_once(reply: Option<&'static [u8]>) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        thread::spawn(move || {
            let _ = listener.set_nonblocking(false);
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 256];
                let _ = stream.set_read_timeout(Some(Duration::from_millis(200)));
                let _ = std::io::Read::read(&mut stream, &mut buf);
                if let Some(bytes) = reply {
                    let _ = stream.write_all(bytes);
                }
            }
        });
        // Brief settle so accept is armed before the probe connects.
        thread::sleep(Duration::from_millis(20));
        addr
    }

    #[test]
    fn http_response_status_requires_status_line_not_substring() {
        assert_eq!(http_response_status("HTTP/1.1 200 OK\r\n"), Some(200));
        assert_eq!(
            http_response_status("HTTP/1.0 204 No Content\r\n"),
            Some(204)
        );
        assert_eq!(
            http_response_status("HTTP/1.1 503 Unavailable\r\n"),
            Some(503)
        );
        // Incidental "200" in a non-status body must not parse as success.
        assert_eq!(http_response_status("error code 200 in payload"), None);
        assert_eq!(http_response_status("Content-Length: 200\r\n"), None);
        assert_eq!(http_response_status(""), None);
    }

    #[test]
    fn check_resonate_unreachable_is_not_ok() {
        // Bind then drop so we probe a free port → ECONNREFUSED → ok:false.
        let free = {
            let l = TcpListener::bind("127.0.0.1:0").expect("bind free");
            let a = l.local_addr().expect("addr");
            drop(l);
            a
        };
        let r = check_resonate_at(free);
        assert!(!r.ok, "unreachable must be ok:false, got: {}", r.detail);
        assert!(
            !r.detail.contains("mail-only"),
            "detail must not claim a mail-only skip that does not exist: {}",
            r.detail
        );
    }

    #[test]
    fn check_resonate_non_http_body_is_not_ok() {
        let addr = serve_once(Some(b"not-an-http-server\nerror 200\n"));
        let r = check_resonate_at(addr);
        assert!(!r.ok, "non-HTTP body must be ok:false, got: {}", r.detail);
    }

    #[test]
    fn check_resonate_http_5xx_is_not_ok() {
        let addr = serve_once(Some(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        ));
        let r = check_resonate_at(addr);
        assert!(!r.ok, "HTTP 5xx must be ok:false, got: {}", r.detail);
        assert!(
            r.detail.contains("503"),
            "detail should name the status: {}",
            r.detail
        );
    }

    #[test]
    fn check_resonate_http_2xx_is_ok() {
        let addr = serve_once(Some(
            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
        ));
        let r = check_resonate_at(addr);
        assert!(r.ok, "HTTP 200 must be ok:true, got: {}", r.detail);
    }
}

#[cfg(test)]
mod reconstruct_tests {
    //! ADR-0004 Ship 3 (bd-jwa): `toron doctor reconstruct`. Exercises the
    //! rebuild-from-Git-archive path through the CLI orchestration, reusing
    //! the proven `rebuild_index_from_git` (sc14a_dr_rebuild is the parity
    //! proof). Tests call the internal async fns directly (no subprocess).
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use toron_core::index::rebuild::{
        MailRecord, build_fresh_index, inspect_index as core_inspect,
    };

    static SEQ: AtomicU64 = AtomicU64::new(0);

    fn scratch(name: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("toron-recon-{name}-{}-{n}", std::process::id()))
    }

    fn sample_records() -> Vec<MailRecord> {
        vec![
            MailRecord {
                event_id: "r1".into(),
                project_slug: "demo".into(),
                sender: "PiAlpha".into(),
                sender_pubkey: "aa".into(),
                subject: "Review".into(),
                body: "First".into(),
                importance: "normal".into(),
                ack_required: true,
                thread_id: None,
                reply_to: None,
                to_list: vec!["BlueLake".into()],
                cc_list: vec![],
                created_ts_us: 1_700_000_000_000_000,
                local_seq: 1,
            },
            MailRecord {
                event_id: "r2".into(),
                project_slug: "demo".into(),
                sender: "BlueLake".into(),
                sender_pubkey: "bb".into(),
                subject: "Re: Review".into(),
                body: "Second".into(),
                importance: "normal".into(),
                ack_required: false,
                thread_id: Some("r1".into()),
                reply_to: Some("r1".into()),
                to_list: vec!["PiAlpha".into()],
                cc_list: vec![],
                created_ts_us: 1_700_000_000_100_000,
                local_seq: 2,
            },
        ]
    }

    fn seed_archive(root: &Path, recs: &[MailRecord]) {
        for rec in recs {
            rec.archive_to(root).expect("archive rec");
        }
    }

    async fn build_index(root: &Path) {
        let db = root.join("index.sqlite");
        build_fresh_index(db.to_str().unwrap(), root)
            .await
            .expect("build index");
    }

    #[tokio::test]
    async fn reconstruct_restores_deleted_index_from_archive() {
        let dir = scratch("restore");
        let root = dir.join("demo");
        std::fs::create_dir_all(&root).unwrap();
        seed_archive(&root, &sample_records());
        build_index(&root).await; // consistent baseline
        std::fs::remove_file(root.join("index.sqlite")).unwrap(); // simulate loss

        let report = reconstruct_project(&root, false).await.unwrap();
        assert_eq!(report.status, "rebuilt");
        assert_eq!(report.rebuilt_rows, Some(2));
        assert!(
            report.quarantine.is_none(),
            "no quarantine when db was absent"
        );

        let (exists, ok, rows) = core_inspect(&root.join("index.sqlite")).await.unwrap();
        assert!(exists && ok, "rebuilt db is present + integral");
        assert_eq!(rows, Some(2), "converged to archive count");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn reconstruct_leaves_healthy_index_untouched() {
        let dir = scratch("healthy");
        let root = dir.join("demo");
        std::fs::create_dir_all(&root).unwrap();
        seed_archive(&root, &sample_records());
        build_index(&root).await; // consistent

        let report = reconstruct_project(&root, false).await.unwrap();
        assert_eq!(report.status, "healthy");
        assert!(report.quarantine.is_none());
        // No forensic bundle created.
        let no_forensic = std::fs::read_dir(&root).unwrap().flatten().all(|e| {
            !e.file_name()
                .to_string_lossy()
                .starts_with("doctor-forensic")
        });
        assert!(no_forensic, "healthy index must not be mutated");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn reconstruct_quarantines_and_rebuilds_on_drift() {
        let dir = scratch("drift");
        let root = dir.join("demo");
        std::fs::create_dir_all(&root).unwrap();
        let mut recs = sample_records();
        seed_archive(&root, &recs);
        build_index(&root).await; // consistent at 2

        // Cause drift: add a 3rd archived message WITHOUT updating the index.
        recs.push(MailRecord {
            event_id: "r3".into(),
            project_slug: "demo".into(),
            sender: "PiAlpha".into(),
            sender_pubkey: "aa".into(),
            subject: "Third".into(),
            body: "Drift".into(),
            importance: "normal".into(),
            ack_required: false,
            thread_id: None,
            reply_to: None,
            to_list: vec![],
            cc_list: vec![],
            created_ts_us: 1_700_000_000_200_000,
            local_seq: 3,
        });
        seed_archive(&root, &recs[2..]); // archive now has 3; index still 2

        let report = reconstruct_project(&root, false).await.unwrap();
        assert_eq!(report.status, "rebuilt");
        assert_eq!(report.rebuilt_rows, Some(3));
        let q = report
            .quarantine
            .as_ref()
            .expect("db existed → quarantined");
        let qpath = PathBuf::from(q);
        assert!(
            qpath.join("index.sqlite.orig").exists(),
            "original db preserved in bundle"
        );
        assert!(qpath.join("manifest.json").exists(), "manifest written");

        let (_, _, rows) = core_inspect(&root.join("index.sqlite")).await.unwrap();
        assert_eq!(rows, Some(3), "live db converged to 3");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn reconstruct_dry_run_makes_no_changes() {
        let dir = scratch("dryrun");
        let root = dir.join("demo");
        std::fs::create_dir_all(&root).unwrap();
        seed_archive(&root, &sample_records());
        build_index(&root).await;
        std::fs::remove_file(root.join("index.sqlite")).unwrap(); // needs rebuild

        let report = reconstruct_project(&root, true).await.unwrap();
        assert_eq!(report.status, "would_rebuild");
        assert!(report.quarantine.is_none(), "dry-run must not quarantine");
        assert!(
            !root.join("index.sqlite").exists(),
            "dry-run must not build"
        );
        assert!(report.reason.is_some(), "dry-run explains why");

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn classify_exit_maps_reports_correctly() {
        // Diagnostic when nothing resolvable.
        assert_eq!(classify_exit(&[], false, 0), ReconstructExit::Diagnostic);
        assert_eq!(classify_exit(&[], true, 0), ReconstructExit::Diagnostic);
        // Ok when all healthy/rebuilt.
        let ok = vec![ReconstructReport {
            slug: "a".into(),
            archive_messages: 1,
            index_rows_before: Some(1),
            rebuilt_rows: None,
            quarantine: None,
            status: "healthy".into(),
            reason: None,
        }];
        assert_eq!(classify_exit(&ok, true, 1), ReconstructExit::Ok);
        // Failed when any project failed (quarantine preserved).
        let mut mixed = ok.clone();
        mixed.push(ReconstructReport {
            slug: "b".into(),
            archive_messages: 1,
            index_rows_before: None,
            rebuilt_rows: None,
            quarantine: Some("x".into()),
            status: "failed".into(),
            reason: Some("verify mismatch".into()),
        });
        assert_eq!(classify_exit(&mixed, true, 2), ReconstructExit::Failed);
    }

    #[test]
    fn run_doctor_reconstruct_missing_data_dir_exits_diagnostic() {
        // Env-independent Diagnostic path: an explicitly-passed data dir that
        // does not exist resolves to `resolved=false` → exit 3. (The env
        // fallback branch is not testable here without `set_var`, which is
        // unsafe in edition 2024 and toron forbids unsafe; the precedence
        // rules are covered by `resolve_data_dir_prefers_explicit`.)
        let cfg = Config::default();
        let args = DoctorReconstructArgs {
            dry_run: false,
            project: None,
            data_dir: Some(PathBuf::from("/nonexistent/definitely-missing-toron-data")),
        };
        let code = run_doctor_reconstruct(&cfg, &args, OutputFormat::Json).unwrap();
        assert_eq!(code, ExitCode::from(3));
    }

    #[test]
    fn resolve_data_dir_prefers_explicit_override() {
        // --data-dir beats env + config, independent of ambient env state.
        let cfg = Config::default();
        let explicit = PathBuf::from("/explicit/path");
        assert_eq!(
            resolve_data_dir(&cfg, Some(&explicit)),
            Some(explicit.clone())
        );
        // No override and no config: env is consulted, then the durable XDG
        // root — never None (R2 / ADR-0007 D3 kills the temp default).
        let expected = std::env::var_os("TORON_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(toron_core::config::durable_data_dir);
        assert_eq!(resolve_data_dir(&cfg, None), Some(expected));
    }

    #[test]
    fn run_doctor_reconstruct_success_exits_zero() {
        // Set up a project archive with a deleted index under a temp data dir.
        let dir = scratch("runok");
        let root = dir.join("demo");
        std::fs::create_dir_all(&root).unwrap();
        // Seed archive synchronously via MailRecord::archive_to.
        for rec in sample_records() {
            rec.archive_to(&root).unwrap();
        }
        // No index yet → reconstruct will build it.
        let cfg = Config::default();
        let args = DoctorReconstructArgs {
            dry_run: false,
            project: None,
            data_dir: Some(dir.clone()),
        };
        let code = run_doctor_reconstruct(&cfg, &args, OutputFormat::Json).unwrap();
        assert_eq!(code, ExitCode::SUCCESS, "rebuilt successfully → exit 0");
        assert!(root.join("index.sqlite").exists(), "index built in place");
        let _ = std::fs::remove_dir_all(dir);
    }
}
