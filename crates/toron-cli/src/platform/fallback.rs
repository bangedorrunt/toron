// governed-by: ADR-0031

use std::path::Path;
use std::process::Command;

use crate::cli::DaemonArgs;

use super::{DaemonState, InstallCtx};

/// No supervisor on fallback hosts — always `None` so `daemon.rs` goes
/// straight to pidfile + addr probes.
pub(crate) fn detect_supervised(_label: &str) -> Option<DaemonState> {
    None
}

/// Fallback install: pidfile mode needs no unit — point at `daemon start`.
pub(crate) fn install_supervised(ctx: &InstallCtx<'_>) -> anyhow::Result<()> {
    let _ = (
        ctx.key_path,
        ctx.out_log,
        ctx.err_log,
        ctx.addr,
        ctx.label,
        ctx.binary,
        ctx.state,
    );
    anyhow::bail!("daemon install: unsupported host — use `toron daemon start` (pidfile mode)")
}

/// Fallback uninstall: SIGTERM the pidfile pid + remove the pidfile.
pub(crate) fn uninstall_supervised(args: &DaemonArgs) -> anyhow::Result<()> {
    use crate::daemon::{default_state_dir, process_alive, read_pidfile};
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let label = args.label.clone();
    let pidfile = state.join("toron.pid");
    if let Some(pid) = read_pidfile(&pidfile) {
        if process_alive(pid) {
            unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        }
    }
    let _ = std::fs::remove_file(&pidfile);
    println!("uninstalled {label} (pidfile mode)");
    Ok(())
}

/// Fallback start: detached pidfile spawn.
pub(crate) fn start_supervised(args: &DaemonArgs, pidfile: &Path) -> anyhow::Result<()> {
    use crate::daemon::default_state_dir;
    let state = args.state_dir.clone().unwrap_or_else(default_state_dir);
    let label = args.label.clone();
    spawn_pidfile(&state, &args.addr)?;
    println!("started {label} (pidfile {})", pidfile.display());
    Ok(())
}

/// Fallback stop: SIGTERM the pidfile pid. Complete — `daemon.rs` does
/// nothing after this returns.
pub(crate) fn stop_supervised(args: &DaemonArgs, pidfile: &Path) -> anyhow::Result<()> {
    use crate::daemon::{process_alive, read_pidfile};
    let label = args.label.clone();
    if let Some(pid) = read_pidfile(pidfile) {
        if process_alive(pid) {
            let _ = Command::new("kill").arg(pid.to_string()).output();
            println!("stopped {label} (pid {pid})");
            return Ok(());
        }
    }
    println!("{label} not running");
    Ok(())
}

/// Detached pidfile spawn (fallback hosts): double-fork-ish via `setsid`
/// (Command pre_exec), logs to the state dir.
fn spawn_pidfile(state: &Path, addr: &str) -> anyhow::Result<()> {
    let binary = std::env::current_exe()?;
    let out = std::fs::File::create(state.join("serve.out.log"))?;
    let err = std::fs::File::create(state.join("serve.err.log"))?;
    let child = Command::new(binary)
        .args(["serve", "--mcp-http", addr, "--no-color"])
        .stdout(std::os::unix::process::CommandExt::stdout(out))
        .stderr(std::os::unix::process::CommandExt::stderr(err))
        .spawn()?;
    let pidfile = state.join("toron.pid");
    std::fs::write(&pidfile, child.id().to_string())?;
    Ok(())
}
