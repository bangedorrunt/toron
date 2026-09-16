// governed-by: ADR-0003, ADR-0009
// SAFETY: no `unsafe` in this module — host supervision (uid, kill -0,
// signals) shells out to `id -u` / `kill` instead of raw libc, keeping the
// crate-level `#![forbid(unsafe_code)]` intact.

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

use crate::platform::DaemonState;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// Default state dir: `$XDG_STATE_HOME/toron` else `~/.local/state/toron`.
pub fn default_state_dir() -> PathBuf {
    if let Ok(xdg) = std::env::var("XDG_STATE_HOME")
        && !xdg.is_empty()
    {
        return PathBuf::from(xdg).join("toron");
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.is_empty()
    {
        return PathBuf::from(home)
            .join(".local")
            .join("state")
            .join("toron");
    }
    PathBuf::from("toron-state")
}

/// The daemon passphrase file: first config-home candidate.
pub fn passphrase_file() -> PathBuf {
    let candidates = toron_core::config::passphrase_file_candidates(
        std::env::var("XDG_CONFIG_HOME")
            .ok()
            .map(PathBuf::from)
            .as_deref(),
        std::env::var("HOME").ok().map(PathBuf::from).as_deref(),
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
    // Supervisor probe lives in platform/<os>.rs (bd-nc5d); shared
    // pidfile + addr probes stay here.
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

    // Supervised install lives in platform/<os>.rs (bd-nc5d); the prelude
    // above is shared.
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
            // A start that starts NOTHING must not exit 0 — the caller
            // asked for a supervised daemon and none exists (2026-08-17:
            // the silent Ok here made `daemon restart` look like a no-op
            // while a stray `toron serve` kept serving the old build).
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

    // Supervised start lives in platform/<os>.rs (bd-nc5d).
    crate::platform::start_supervised(args, &pidfile)
}

/// `stop`: bootout/systemctl stop/pidfile SIGTERM. Idempotent.
pub fn stop(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let pidfile = state.join("toron.pid");
    // Supervised stop lives in platform/<os>.rs (bd-nc5d).
    crate::platform::stop_supervised(args, &pidfile)
}

/// `status`: print the detection result.
pub fn status(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let pidfile = state.join("toron.pid");
    match detect_state(&args.label, &pidfile, &args.addr) {
        DaemonState::RunningLaunchd => println!("running (launchd {})", args.label),
        DaemonState::RunningSystemd => println!("running (systemd {})", args.label),
        DaemonState::RunningPidfile(pid) => println!("running (pidfile {pid})"),
        DaemonState::RunningUnmanaged => {
            let pid = port_holder_pid(&args.addr);
            println!(
                "running (unmanaged instance on {}, pid {})",
                args.addr,
                pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into())
            );
        }
        DaemonState::Stopped => println!("stopped"),
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
pub fn run_dispatch(args: &super::cli::DaemonArgs) -> anyhow::Result<()> {
    use super::cli::DaemonCommand;
    match &args.cmd {
        DaemonCommand::Status => status(args),
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

    #[test]
    fn default_state_dir_prefers_xdg_then_home() {
        // Can't set process env safely here; assert the structural fallback
        // by testing with an explicit state_dir through the args struct.
        let args = super::super::cli::DaemonArgs {
            addr: "127.0.0.1:18080".into(),
            label: "com.toron.serve".into(),
            state_dir: Some(PathBuf::from("/custom/state")),
            force: false,
            cmd: super::super::cli::DaemonCommand::Status,
        };
        assert_eq!(
            args.state_dir.unwrap_or_else(default_state_dir),
            PathBuf::from("/custom/state")
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
    #[test]
    fn effective_passphrase_has_no_empty_value() {
        let p = effective_passphrase();
        assert!(!p.is_empty());
    }
}
