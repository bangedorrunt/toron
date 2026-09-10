// governed-by: ADR-0031
// Linux supervision (bd-nc5d): verbatim move of the linux-gated blocks out
// of `daemon.rs`. No signature or behavior changes — this module is only
// compiled on Linux, so it carries zero cfg attributes.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::DaemonArgs;

use super::{DaemonState, InstallCtx};

/// Probe the systemd user unit; `Some(RunningSystemd)` when active, else
/// `None` so `daemon.rs` falls through to pidfile + addr probes.
pub(crate) fn detect_supervised(label: &str) -> Option<DaemonState> {
    let out = Command::new("systemctl")
        .args(["--user", "is-active", label])
        .output();
    if let Ok(o) = out
        && String::from_utf8_lossy(&o.stdout).trim() == "active"
    {
        return Some(DaemonState::RunningSystemd);
    }
    None
}

/// systemd install: write the user unit, daemon-reload + enable + start.
pub(crate) fn install_supervised(ctx: &InstallCtx<'_>) -> anyhow::Result<()> {
    let label = ctx.label;
    let unit = systemd_unit(
        label,
        ctx.binary,
        ctx.addr,
        ctx.key_path,
        ctx.pass,
        ctx.state,
    );
    let dir = home_systemd_user()?;
    std::fs::create_dir_all(&dir)?;
    let unit_path = dir.join(format!("{label}.service"));
    std::fs::write(&unit_path, unit)?;
    for c in [
        Command::new("systemctl").args(["--user", "daemon-reload"]),
        Command::new("systemctl").args(["--user", "enable", label]),
        Command::new("systemctl").args(["--user", "start", label]),
    ] {
        let out = c.output()?;
        if !out.status.success() {
            anyhow::bail!(
                "systemctl {:?} failed: {}",
                c.get_args().collect::<Vec<_>>(),
                String::from_utf8_lossy(&out.stderr)
            );
        }
    }
    println!("installed + started {label} (unit {})", unit_path.display());
    Ok(())
}

/// systemd uninstall: stop + disable + remove the unit (idempotent).
pub(crate) fn uninstall_supervised(args: &DaemonArgs) -> anyhow::Result<()> {
    let label = args.label.clone();
    for c in [
        Command::new("systemctl").args(["--user", "stop", &label]),
        Command::new("systemctl").args(["--user", "disable", &label]),
    ] {
        let _ = c.output();
    }
    let unit = home_systemd_user()?.join(format!("{label}.service"));
    if unit.is_file() {
        let _ = std::fs::remove_file(&unit);
    }
    Ok(())
}

/// systemd start: start, else install (idempotent) then start.
pub(crate) fn start_supervised(args: &DaemonArgs, _pidfile: &Path) -> anyhow::Result<()> {
    let label = args.label.clone();
    let out = Command::new("systemctl")
        .args(["--user", "start", &label])
        .output()?;
    if !out.status.success() {
        crate::daemon::install(args)?;
    }
    let out = Command::new("systemctl")
        .args(["--user", "start", &label])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "systemctl start failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    println!("started {label} (systemd)");
    Ok(())
}

/// systemd stop: unit stop (idempotent). Complete — `daemon.rs` does nothing
/// after this returns.
pub(crate) fn stop_supervised(args: &DaemonArgs, _pidfile: &Path) -> anyhow::Result<()> {
    let label = args.label.clone();
    let out = Command::new("systemctl")
        .args(["--user", "stop", &label])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "systemctl stop failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    println!("stopped {label} (systemd)");
    Ok(())
}

/// Render the systemd user unit (pure — golden-tested). Only reachable from
/// linux install paths (and unit tests).
pub fn systemd_unit(
    label: &str,
    binary: &Path,
    addr: &str,
    key_path: &Path,
    passphrase_file: &Path,
    state_dir: &Path,
) -> String {
    let out = state_dir.join("serve.out.log");
    let err = state_dir.join("serve.err.log");
    format!(
        "[Unit]\n\
         Description={label} — toron Nostr-native agent mailbox daemon\n\
         After=network-online.target\n\
         Wants=network-online.target\n\
         \n\
         [Service]\n\
         ExecStart={} serve --mcp-http {addr} --no-color\n\
         Environment=TORON_OPERATOR_KEY={}\n\
         Environment=TORON_KEY_PASSPHRASE_FILE={}\n\
         Environment=RUST_LOG=warn\n\
         Restart=on-failure\n\
         RestartSec=5\n\
         StandardOutput=append:{}\n\
         StandardError=append:{}\n\
         \n\
         [Install]\n\
         WantedBy=default.target\n",
        binary.display(),
        key_path.display(),
        passphrase_file.display(),
        out.display(),
        err.display(),
    )
}

fn home_systemd_user() -> anyhow::Result<PathBuf> {
    if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME")
        && !xdg.is_empty()
    {
        return Ok(PathBuf::from(xdg).join("systemd").join("user"));
    }
    let home = std::env::var("HOME")
        .map_err(|_| anyhow::anyhow!("HOME unset; cannot locate systemd user units"))?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("systemd")
        .join("user"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_golden_shape_and_no_secret_inline() {
        let unit = systemd_unit(
            "com.toron.serve",
            Path::new("/usr/local/bin/toron"),
            "127.0.0.1:18080",
            Path::new("/home/x/.config/toron/operator_nsec"),
            Path::new("/home/x/.config/toron/passphrase"),
            Path::new("/home/x/.local/state/toron"),
        );
        assert!(unit.contains(
            "ExecStart=/usr/local/bin/toron serve --mcp-http 127.0.0.1:18080 --no-color"
        ));
        assert!(unit.contains("TORON_KEY_PASSPHRASE_FILE=/home/x/.config/toron/passphrase"));
        assert!(!unit.contains("TORON_KEY_PASSPHRASE="), "no inline secret");
        assert!(
            unit.contains("Restart=on-failure"),
            "no respawn after clean exit"
        );
        assert!(!unit.contains("Restart=always"));
        assert!(unit.contains("WantedBy=default.target"));
    }
}
