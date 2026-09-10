// governed-by: ADR-0031
// macOS supervision (bd-nc5d): verbatim move of the macos-gated blocks out
// of `daemon.rs`. No signature or behavior changes — this module is only
// compiled on macOS, so it carries zero cfg attributes.

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::DaemonArgs;

use super::{DaemonState, InstallCtx};

/// Probe the launchd job; `Some(RunningLaunchd)` when loaded, else `None`
/// so `daemon.rs` falls through to pidfile + addr probes.
pub(crate) fn detect_supervised(label: &str) -> Option<DaemonState> {
    let uid = current_uid();
    let out = Command::new("launchctl")
        .args(["print", &format!("gui/{uid}/{label}")])
        .output();
    if let Ok(o) = out
        && o.status.success()
    {
        return Some(DaemonState::RunningLaunchd);
    }
    None
}

/// launchd install: write the plist + logrotate agent, bootstrap + enable +
/// kickstart (idempotent).
pub(crate) fn install_supervised(ctx: &InstallCtx<'_>) -> anyhow::Result<()> {
    let label = ctx.label;
    let plist = plist_xml(
        label,
        ctx.binary,
        ctx.addr,
        ctx.key_path,
        ctx.pass,
        ctx.out_log,
        ctx.err_log,
    );
    let dir = home_launch_agents()?;
    std::fs::create_dir_all(&dir)?;
    let plist_path = dir.join(format!("{label}.plist"));
    std::fs::write(&plist_path, plist)?;
    let uid = current_uid();
    let out = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();
    let _ = out; // absent unit is fine
    install_logrotate_agent(&dir, &uid)?;
    let out = Command::new("launchctl")
        .args([
            "bootstrap",
            &format!("gui/{uid}"),
            plist_path.to_str().unwrap_or_default(),
        ])
        .output()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        // "already bootstrapped" is fine (idempotent re-install).
        if !stderr.contains("already") && !stderr.contains("Bootstrap failed: 17") {
            anyhow::bail!("launchctl bootstrap failed: {stderr}");
        }
    }
    let out = Command::new("launchctl")
        .args(["enable", &format!("gui/{uid}/{label}")])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "launchctl enable failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let out = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("gui/{uid}/{label}")])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "launchctl kickstart failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    println!(
        "installed + started {label} (plist {})",
        plist_path.display()
    );
    Ok(())
}

/// launchd uninstall: bootout + remove the plist (idempotent).
pub(crate) fn uninstall_supervised(args: &DaemonArgs) -> anyhow::Result<()> {
    let label = args.label.clone();
    let uid = current_uid();
    let out = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "launchctl bootout failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let plist = home_launch_agents()?.join(format!("{label}.plist"));
    if plist.is_file() {
        std::fs::remove_file(&plist)?;
    }
    println!("uninstalled {label}");
    Ok(())
}

/// launchd start: kickstart, else install (idempotent) then kickstart.
pub(crate) fn start_supervised(args: &DaemonArgs, _pidfile: &Path) -> anyhow::Result<()> {
    let label = args.label.clone();
    let uid = current_uid();
    let out = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("gui/{uid}/{label}")])
        .output();
    if let Ok(o) = out
        && o.status.success()
    {
        println!("started {label} (launchd)");
        return Ok(());
    }
    // Not loaded yet — install (idempotent) then kickstart.
    crate::daemon::install(args)?;
    let out = Command::new("launchctl")
        .args(["kickstart", "-k", &format!("gui/{uid}/{label}")])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "launchctl kickstart failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    println!("started {label} (launchd)");
    Ok(())
}

/// launchd stop: bootout (+ port grace), pidfile kill, unmanaged/force
/// handling. Complete — `daemon.rs` does nothing after this returns.
pub(crate) fn stop_supervised(args: &DaemonArgs, pidfile: &Path) -> anyhow::Result<()> {
    use crate::daemon::{addr_alive, port_holder_pid, process_alive, read_pidfile};
    let label = args.label.clone();
    let uid = current_uid();
    let out = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();
    if let Ok(o) = out
        && o.status.success()
    {
        // launchd job booted out. The serve process needs a moment to
        // release the port — probe with a grace window before deciding
        // a stray unmanaged holder exists (a fresh-bootout socket still
        // accepting is a teardown race, not a holder).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while addr_alive(&args.addr) && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(250));
        }
        if !addr_alive(&args.addr) {
            println!("stopped {label} (launchd)");
            return Ok(());
        }
    }
    if let Some(pid) = read_pidfile(pidfile)
        && process_alive(pid)
    {
        let _ = Command::new("kill").arg(pid.to_string()).output();
        println!("stopped {label} (pid {pid})");
        return Ok(());
    }
    if addr_alive(&args.addr) {
        let pid = port_holder_pid(&args.addr);
        if args.force
            && let Some(p) = pid
        {
            let _ = Command::new("kill").arg(p.to_string()).output();
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            while addr_alive(&args.addr) && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(200));
            }
            if addr_alive(&args.addr) {
                anyhow::bail!(
                    "unmanaged pid {p} still holds {} after SIGTERM — kill it manually",
                    args.addr
                );
            }
            println!("stopped unmanaged instance (pid {p})");
            return Ok(());
        }
        anyhow::bail!(
            "{label} not managed (launchd/pidfile) but {} responds (unmanaged pid {}) — \
             refusing to kill it; `toron daemon stop --force` to replace it",
            args.addr,
            pid.map(|p| p.to_string()).unwrap_or_else(|| "?".into())
        );
    }
    println!("{label} not running");
    Ok(())
}

/// Current uid via `id -u` (keeps the crate unsafe-free).
pub fn current_uid() -> String {
    let out = Command::new("id").arg("-u").output();
    out.ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "501".to_string())
}

/// Render the launchd plist (pure — golden-tested).
pub fn plist_xml(
    label: &str,
    binary: &Path,
    addr: &str,
    key_path: &Path,
    passphrase_file: &Path,
    out_log: &Path,
    err_log: &Path,
) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>{label}</string>
	<key>ProgramArguments</key>
	<array>
		<string>{}</string>
		<string>serve</string>
		<string>--mcp-http</string>
		<string>{addr}</string>
		<string>--no-color</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<dict>
		<key>SuccessfulExit</key>
		<false/>
	</dict>
	<key>ThrottleInterval</key>
	<integer>5</integer>
	<key>StandardOutPath</key>
	<string>{}</string>
	<key>StandardErrorPath</key>
	<string>{}</string>
	<key>EnvironmentVariables</key>
	<dict>
		<key>TORON_OPERATOR_KEY</key>
		<string>{}</string>
		<key>TORON_KEY_PASSPHRASE_FILE</key>
		<string>{}</string>
		<key>RUST_LOG</key>
		<string>warn</string>
	</dict>
</dict>
</plist>
"#,
        binary.display(),
        out_log.display(),
        err_log.display(),
        key_path.display(),
        passphrase_file.display(),
    )
}

/// Hourly copy-truncate of toron + resonate logs (bd-741i).
///
/// Weekly rotation left `resonate.out.log` at 1.3GB (`RUST_LOG=info` doorbell
/// poll-fail storm). Hourly + 1 MiB threshold caps both daemons. Pure —
/// golden-tested.
pub fn logrotate_plist_xml() -> String {
    let script = r#"for f in "$HOME/.local/state/toron/serve.out.log" "$HOME/.local/state/toron/serve.err.log" "$HOME/.local/state/flywheel/resonate.out.log" "$HOME/.local/state/flywheel/resonate.err.log"; do [ -f "$f" ] || continue; sz=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f"); if [ "$sz" -gt 1048576 ]; then cp "$f" "$f.1" && : > "$f"; fi; done"#;
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>com.toron.logrotate</string>
	<key>ProgramArguments</key>
	<array>
		<string>/bin/sh</string>
		<string>-c</string>
		<string>{script}</string>
	</array>
	<key>StartInterval</key>
	<integer>3600</integer>
</dict>
</plist>
"#
    )
}

/// Write + bootstrap `com.toron.logrotate` (hourly copy-truncate, bd-741i).
fn install_logrotate_agent(dir: &Path, uid: &str) -> anyhow::Result<()> {
    let label = "com.toron.logrotate";
    let path = dir.join(format!("{label}.plist"));
    std::fs::write(&path, logrotate_plist_xml())?;
    let _ = Command::new("launchctl")
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();
    let out = Command::new("launchctl")
        .args([
            "bootstrap",
            &format!("gui/{uid}"),
            path.to_str().unwrap_or_default(),
        ])
        .output()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if !stderr.contains("already") && !stderr.contains("Bootstrap failed: 17") {
            anyhow::bail!("launchctl bootstrap {label} failed: {stderr}");
        }
    }
    Ok(())
}

fn home_launch_agents() -> anyhow::Result<PathBuf> {
    let home = std::env::var("HOME")
        .map_err(|_| anyhow::anyhow!("HOME unset; cannot locate LaunchAgents"))?;
    Ok(PathBuf::from(home).join("Library").join("LaunchAgents"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plist_golden_shape_and_no_secret_inline() {
        let xml = plist_xml(
            "com.toron.serve",
            Path::new("/usr/local/bin/toron"),
            "127.0.0.1:18080",
            Path::new("/Users/x/.config/toron/operator_nsec"),
            Path::new("/Users/x/.config/toron/passphrase"),
            Path::new("/Users/x/.local/state/toron/serve.out.log"),
            Path::new("/Users/x/.local/state/toron/serve.err.log"),
        );
        assert!(xml.contains("<string>com.toron.serve</string>"), "label");
        assert!(xml.contains("<string>--mcp-http</string>"), "http flag");
        assert!(xml.contains("<string>127.0.0.1:18080</string>"), "addr");
        assert!(xml.contains("<key>KeepAlive</key>"), "autostart semantics");
        assert!(
            xml.contains("<key>SuccessfulExit</key>\n\t\t<false/>"),
            "bd-serve-double-spawn-oqe6: respawn on failure only — a clean \
             exit (another instance already serving) must stay dead"
        );
        assert!(!xml.contains("<key>KeepAlive</key>\n\t<true/>"));
        assert!(xml.contains("<key>RunAtLoad</key>"), "run at load");
        assert!(
            xml.contains("TORON_KEY_PASSPHRASE_FILE"),
            "file-based secret"
        );
        assert!(
            !xml.contains("TORON_KEY_PASSPHRASE</key>"),
            "no inline passphrase env"
        );
        assert!(
            !xml.contains("<string>toron</string>"),
            "no literal secret value in the unit"
        );
    }

    #[test]
    fn logrotate_plist_is_hourly_and_covers_resonate() {
        let xml = logrotate_plist_xml();
        assert!(xml.contains("<string>com.toron.logrotate</string>"));
        assert!(
            xml.contains("<key>StartInterval</key>\n\t<integer>3600</integer>"),
            "hourly, not weekly calendar"
        );
        assert!(!xml.contains("StartCalendarInterval"));
        assert!(xml.contains("resonate.out.log"), "bd-741i: resonate log");
        assert!(xml.contains("serve.out.log"));
        assert!(xml.contains("1048576"), "1 MiB copy-truncate threshold");
    }
}
