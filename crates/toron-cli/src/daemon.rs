// governed-by: ADR-0003, ADR-0009
// SAFETY: no `unsafe` in this module — host supervision (uid, kill -0,

//! Daemon lifecycle: `toron daemon start|stop|restart|status|install|uninstall`.
//!
//! Supervision strategy, per host:
//! - macOS: launchd LaunchAgent (`~/Library/LaunchAgents/<label>.plist`,
//!   `launchctl bootstrap`/`bootout`/`kickstart`).
//! - Linux with systemd: user unit (`~/.config/systemd/user/<label>.service`,
//!   `systemctl --user`).
//! - Fallback: pidfile + detached `toron serve --mcp-http` spawn
//!   (`~/.local/state/toron/toron.pid`).
//!
//! The passphrase NEVER appears in the unit: `install` writes
//! `~/.config/toron/passphrase` (0600) and the unit sets
//! `TORON_KEY_PASSPHRASE_FILE` instead of `TORON_KEY_PASSPHRASE`.

use crate::output::OutputFormat;
use crate::platform::DaemonState;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;
use toron_core::serve_build::{self, BuildVerdict};

/// The daemon's default MCP-over-HTTP address. One constant because `doctor`
/// probes the same address the daemon binds (a second literal drifts).
pub const DEFAULT_ADDR: &str = "127.0.0.1:18080";

/// Default state dir: `$XDG_STATE_HOME/toron` else `~/.local/state/toron`.
///
/// Delegates to [`toron_core::config::state_dir`] — the ONE derivation the
/// serve-side wake sidecar and every consumer's discovery share (ADR-0036
/// Spec 2; bd-ewsi). Two sides deriving independently is the disagreement
/// that starved wake.sock: consumers dialed a path no serve ever bound.
pub fn default_state_dir() -> PathBuf {
    toron_core::config::state_dir()
}

/// The daemon passphrase file: first config-home candidate.
pub fn passphrase_file() -> PathBuf {
    let candidates = toron_core::config::passphrase_file_candidates(
        toron_core::config::home_env("XDG_CONFIG_HOME").as_deref(),
        toron_core::config::home_env("HOME").as_deref(),
    );
    candidates
        .into_iter()
        .next()
        .unwrap_or_else(|| PathBuf::from(".config/toron/passphrase"))
}

/// Effective passphrase for `install`: the resolved passphrase (env/file/
/// default) — persisted to the passphrase file so the unit never needs the
/// secret inline.
pub fn effective_passphrase() -> String {
    toron_core::config::operator_passphrase()
}

/// Read a pidfile into an optional pid (pure — tolerant of junk).
pub fn read_pidfile(path: &Path) -> Option<u32> {
    let s = std::fs::read_to_string(path).ok()?;
    s.trim().parse::<u32>().ok()
}

/// Probe whether a TCP address accepts connections (2s budget).
pub fn addr_alive(addr: &str) -> bool {
    let Ok(sa): Result<SocketAddr, _> = addr.parse() else {
        return false;
    };
    TcpStream::connect_timeout(&sa, Duration::from_millis(2000)).is_ok()
}

/// Detect state: launchd (darwin) / systemd (linux) / pidfile + addr probe.
pub fn detect_state(label: &str, pidfile: &Path, addr: &str) -> DaemonState {
    if let Some(state) = crate::platform::detect_supervised(label) {
        return state;
    }
    if let Some(pid) = read_pidfile(pidfile)
        && process_alive(pid)
    {
        return DaemonState::RunningPidfile(pid);
    }
    if addr_alive(addr) {
        return DaemonState::RunningUnmanaged;
    }
    DaemonState::Stopped
}

/// Whether a pid is a live process on this host (`kill -0` shell-out; no
/// unsafe).
pub fn process_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let out = Command::new("kill").args(["-0", &pid.to_string()]).output();
    matches!(out, Ok(o) if o.status.success())
}

/// Parse the first pid from `lsof -t` output (pure — unit-tested). Tolerates
/// blank lines and multiple pids (first wins; one listener per port here).
pub fn first_pid_from_lsof(out: &str) -> Option<u32> {
    out.lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .and_then(|l| l.parse::<u32>().ok())
}

/// The pid of the process LISTENING on `addr` (lsof). `None` when the port
/// is free or lsof is unavailable. Used to name/kill unmanaged holders.
pub fn port_holder_pid(addr: &str) -> Option<u32> {
    let port = addr.rsplit(':').next()?;
    let out = Command::new("lsof")
        .args(["-nP", "-t", &format!("-iTCP:{port}"), "-sTCP:LISTEN"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    first_pid_from_lsof(&String::from_utf8_lossy(&out.stdout))
}

/// Wait up to `secs` for the address to accept connections (restart verify).
pub fn wait_addr(addr: &str, secs: u64) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(secs);
    while std::time::Instant::now() < deadline {
        if addr_alive(addr) {
            return true;
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    false
}

/// Write `~/.config/toron/passphrase` (0600) if absent — the unit references
/// only this file, never the secret inline. Idempotent.
pub fn ensure_passphrase_file() -> anyhow::Result<PathBuf> {
    let path = passphrase_file();
    if !path.is_file() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let pass = effective_passphrase();
        std::fs::write(&path, format!("{pass}\n"))?;
        set_mode_0600(&path)?;
    }
    Ok(path)
}

#[cfg(unix)]
fn set_mode_0600(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}
#[cfg(not(unix))]
fn set_mode_0600(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

/// `install`: write the unit + passphrase file, then load it (launchd
/// bootstrap / systemctl daemon-reload + enable + start). Idempotent.
pub fn install(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    let binary = std::env::current_exe()?;
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    std::fs::create_dir_all(&state)?;
    let pass = ensure_passphrase_file()?;
    let key_path = toron_core::config::ToronPaths::resolve().operator_key_path();
    let out_log = state.join("serve.out.log");
    let err_log = state.join("serve.err.log");
    let addr = args.addr.clone();
    let label = args.label.clone();

    let ctx = crate::platform::InstallCtx {
        label: &label,
        binary: &binary,
        addr: &addr,
        key_path: &key_path,
        pass: &pass,
        out_log: &out_log,
        err_log: &err_log,
        state: &state,
    };
    crate::platform::install_supervised(&ctx)
}

/// Write + bootstrap `com.toron.logrotate` (hourly copy-truncate, bd-741i).
/// `uninstall`: stop + remove the unit (idempotent).
pub fn uninstall(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    crate::platform::uninstall_supervised(args)
}

/// `start`: install-if-absent, then ensure running (launchctl kickstart /
/// systemctl start / pidfile spawn).
pub fn start(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let pidfile = state.join("toron.pid");
    let label = args.label.clone();

    match detect_state(&label, &pidfile, &args.addr) {
        DaemonState::RunningLaunchd | DaemonState::RunningSystemd => {
            println!("{label} already running (supervised)");
            return Ok(());
        }
        DaemonState::RunningPidfile(pid) => {
            println!("{label} already running (pid {pid})");
            return Ok(());
        }
        DaemonState::RunningUnmanaged => {
            let pid = port_holder_pid(&args.addr);
            anyhow::bail!(
                "{label} already serving {} (unmanaged pid {}) — stop it first, or \
                 `toron daemon restart --force` to replace it",
                args.addr,
                pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into())
            );
        }
        DaemonState::Stopped => {}
    }

    crate::platform::start_supervised(args, &pidfile)
}

/// `stop`: bootout/systemctl stop/pidfile SIGTERM. Idempotent.
pub fn stop(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let pidfile = state.join("toron.pid");
    crate::platform::stop_supervised(args, &pidfile)
}

/// Where a booting `toron serve` leaves the record of what it runs: beside
/// `serve.pid` in the mailbox data root. Single source for the writer's
/// sibling reads (`daemon status`, `doctor`).
pub fn build_record_path() -> PathBuf {
    toron_core::config::ToronPaths::resolve()
        .root
        .join(serve_build::SERVE_BUILD_FILE)
}

/// Unix seconds as RFC3339 UTC. The record stores seconds, so both `daemon
/// status` and `doctor` must render the same instant the same way.
pub fn ts(unix: i64) -> String {
    chrono::DateTime::from_timestamp(unix, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_else(|| format!("unix:{unix}"))
}

/// `?` for an unknown pid (matches the existing daemon wording).
fn disp(pid: Option<u32>) -> String {
    pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into())
}

fn verdict_word(v: BuildVerdict) -> &'static str {
    match v {
        BuildVerdict::Current => "current",
        BuildVerdict::Replaced => "replaced",
        BuildVerdict::NotServing => "not_serving",
    }
}

/// One line naming the served build and, only when it is the problem, the
/// remedy. Both times appear in the replaced case so the reader can see the
/// gap rather than take "stale" on faith.
pub(crate) fn build_detail(
    snap: &serve_build::BuildSnapshot,
    verdict: BuildVerdict,
    serving_pid: Option<u32>,
    on_disk_built_at: Option<i64>,
) -> String {
    match verdict {
        BuildVerdict::Current => format!(
            "build — {} (booted {}, from a build written {})",
            snap.binary.display(),
            ts(snap.started_at_unix),
            ts(snap.built_at_unix)
        ),
        BuildVerdict::Replaced => format!(
            "build — REPLACED: the daemon (pid {}, booted {}) is running a build written \
             {}; the file on disk is a build written {} — so the daemon is not running \
             the build you built; `toron daemon restart`",
            snap.pid,
            ts(snap.started_at_unix),
            ts(snap.built_at_unix),
            on_disk_built_at
                .map(ts)
                .unwrap_or_else(|| "at an unknown time".to_string())
        ),
        BuildVerdict::NotServing => format!(
            "build — record is from pid {} (booted {}, from a build written {}); the \
             address is held by {} now, so it describes an earlier boot",
            snap.pid,
            ts(snap.started_at_unix),
            ts(snap.built_at_unix),
            disp(serving_pid)
        ),
    }
}

/// The build half of a status report, as data. `--json` renders these same
/// facts, never a second query.
#[derive(Debug, serde::Serialize)]
struct BuildFacts {
    binary: Option<String>,
    started_at: Option<String>,
    binary_built_at: Option<String>,
    verdict: Option<&'static str>,
    restart_required: bool,
    detail: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct StatusReport {
    label: String,
    addr: String,
    state: &'static str,
    source: &'static str,
    pid: Option<u32>,
    build: BuildFacts,
    /// The human first line, so `--json` and the human path cannot disagree.
    #[serde(skip)]
    headline: String,
}

fn status_report(args: &super::cli::DaemonArgs) -> StatusReport {
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let pidfile = state.join("toron.pid");
    let (state_word, source, detected_pid, headline) =
        match detect_state(&args.label, &pidfile, &args.addr) {
            DaemonState::RunningLaunchd => (
                "running",
                "launchd",
                None,
                format!("running (launchd {})", args.label),
            ),
            DaemonState::RunningSystemd => (
                "running",
                "systemd",
                None,
                format!("running (systemd {})", args.label),
            ),
            DaemonState::RunningPidfile(pid) => (
                "running",
                "pidfile",
                Some(pid),
                format!("running (pidfile {pid})"),
            ),
            DaemonState::RunningUnmanaged => {
                let pid = port_holder_pid(&args.addr);
                (
                    "running",
                    "unmanaged",
                    pid,
                    format!(
                        "running (unmanaged instance on {}, pid {})",
                        args.addr,
                        disp(pid)
                    ),
                )
            }
            DaemonState::Stopped => ("stopped", "none", None, "stopped".to_string()),
        };

    let serving_pid = if state_word == "running" {
        detected_pid.or_else(|| port_holder_pid(&args.addr))
    } else {
        None
    };

    let build = match serve_build::read(&build_record_path()) {
        Some(snap) => {
            let verdict = serve_build::build_verdict(
                &snap,
                serving_pid,
                serve_build::artifact_id(&snap.binary),
            );
            BuildFacts {
                binary: Some(snap.binary.display().to_string()),
                started_at: Some(ts(snap.started_at_unix)),
                binary_built_at: Some(ts(snap.built_at_unix)),
                verdict: Some(verdict_word(verdict)),
                restart_required: verdict.is_replaced(),
                detail: Some(build_detail(
                    &snap,
                    verdict,
                    serving_pid,
                    serve_build::artifact_mtime_unix(&snap.binary),
                )),
            }
        }
        None => BuildFacts {
            binary: None,
            started_at: None,
            binary_built_at: None,
            verdict: None,
            restart_required: false,
            detail: None,
        },
    };

    StatusReport {
        label: args.label.clone(),
        addr: args.addr.clone(),
        state: state_word,
        source,
        pid: serving_pid,
        build,
        headline,
    }
}

/// `status`: print the detection result AND the build the daemon is serving.
pub fn status(args: &super::cli::DaemonArgs, format: OutputFormat) -> anyhow::Result<()> {
    let report = status_report(args);
    match format {
        OutputFormat::Json | OutputFormat::Plain => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        OutputFormat::Human => {
            println!("{}", report.headline);
            if let Some(detail) = &report.build.detail {
                println!("  {detail}");
            }
        }
    }
    Ok(())
}

/// `restart`: stop, then start, then VERIFY the new daemon actually serves
/// the address (2026-08-17: stop+start alone reported success while a stray
/// unmanaged instance kept serving the old build — the launchd replacement
/// crash-looped on `Address already in use`, invisible to the caller).
pub fn restart(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    stop(args)?;
    if addr_alive(&args.addr) {
        let pid = port_holder_pid(&args.addr);
        anyhow::bail!(
            "{} still responds after stop (unmanaged pid {}) — \
             `toron daemon restart --force`",
            args.addr,
            pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into())
        );
    }
    start(args)?;
    if !wait_addr(&args.addr, 10) {
        anyhow::bail!(
            "restarted {} but it never came up on {} — see ~/.local/state/toron/serve.err.log",
            args.label,
            args.addr
        );
    }
    let pid = port_holder_pid(&args.addr);
    println!(
        "restarted {} on {} (pid {})",
        args.label,
        args.addr,
        pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into())
    );
    Ok(())
}

/// Dispatch a `toron daemon` subcommand.
pub fn run_dispatch(args: &super::cli::DaemonArgs, format: OutputFormat) -> anyhow::Result<()> {
    use super::cli::DaemonCommand;
    match &args.cmd {
        DaemonCommand::Status => status(args, format),
        DaemonCommand::Start => start(args),
        DaemonCommand::Restart => restart(args),
        DaemonCommand::Stop => stop(args),
        DaemonCommand::Install => install(args),
        DaemonCommand::Uninstall => uninstall(args),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pidfile_read_tolerates_junk() {
        let d = std::env::temp_dir().join(format!("toron-dtest-{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        let good = d.join("good.pid");
        std::fs::write(&good, "12345\n").unwrap();
        assert_eq!(read_pidfile(&good), Some(12345));
        let bad = d.join("bad.pid");
        std::fs::write(&bad, "not-a-pid").unwrap();
        assert_eq!(read_pidfile(&bad), None);
        assert_eq!(read_pidfile(&d.join("missing.pid")), None);
    }

    #[test]
    fn addr_alive_detects_listener_and_closed_port() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        assert!(addr_alive(&addr.to_string()), "bound listener is alive");
        drop(listener);
        assert!(
            !addr_alive(&addr.to_string()),
            "dropped listener port is not alive"
        );
    }

    /// Both readers render the same second the same way, or a `doctor` line
    /// and a `status` line describe different moments.
    #[test]
    fn ts_renders_unix_seconds_as_rfc3339_utc() {
        assert_eq!(ts(0), "1970-01-01T00:00:00+00:00");
        assert_eq!(ts(86_400), "1970-01-02T00:00:00+00:00");
        assert!(
            ts(i64::MAX).starts_with("unix:"),
            "seconds that cannot be rendered are named, never guessed"
        );
    }

    /// The remedy appears ONLY for the case that has one. Telling a healthy
    /// daemon to restart trains the reader to ignore the line, and the replaced
    /// case must carry both times so the gap is visible rather than asserted.
    #[test]
    fn build_detail_names_the_remedy_only_when_the_build_was_replaced() {
        let snap = serve_build::BuildSnapshot {
            pid: 73527,
            binary: PathBuf::from("/tmp/toron"),
            started_at_unix: 1_790_766_773,
            built_at_unix: 1_790_766_298,
            ino: 42,
            size: 1_048_576,
        };

        let current = build_detail(
            &snap,
            BuildVerdict::Current,
            Some(73527),
            Some(snap.built_at_unix),
        );
        assert!(!current.contains("REPLACED"));
        assert!(
            !current.contains("restart"),
            "a healthy daemon is offered no remedy: {current}"
        );

        let rebuilt_at = snap.built_at_unix + 300;
        let replaced = build_detail(&snap, BuildVerdict::Replaced, Some(73527), Some(rebuilt_at));
        assert!(replaced.contains("REPLACED"));
        assert!(replaced.contains("toron daemon restart"));
        assert!(
            replaced.contains(&ts(snap.started_at_unix)),
            "the boot time: {replaced}"
        );
        assert!(
            replaced.contains(&ts(snap.built_at_unix)),
            "the artifact it booted from: {replaced}"
        );
        assert!(
            replaced.contains(&ts(rebuilt_at)),
            "and the one on disk now, which is the point: {replaced}"
        );

        let unknown = build_detail(&snap, BuildVerdict::Replaced, Some(73527), None);
        assert!(unknown.contains("unknown"), "{unknown}");

        let not_serving = build_detail(&snap, BuildVerdict::NotServing, Some(999), None);
        assert!(
            not_serving.contains("999") && !not_serving.contains("restart"),
            "an earlier boot is not a stale build: {not_serving}"
        );
    }

    /// bd-daemon-restart: `first_pid_from_lsof` is the pure half of
    /// port_holder_pid — blank lines tolerated, first pid wins, junk → None.
    #[test]
    fn first_pid_from_lsof_parses_and_tolerates_junk() {
        assert_eq!(first_pid_from_lsof("969\n"), Some(969));
        assert_eq!(first_pid_from_lsof("\n\n  4242 \n981\n"), Some(4242));
        assert_eq!(first_pid_from_lsof(""), None);
        assert_eq!(first_pid_from_lsof("not-a-pid"), None);
    }
}
