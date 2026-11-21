// governed-by: ADR-0031

use std::path::{Path, PathBuf};
use std::process::Command;

use crate::cli::DaemonArgs;

use super::{DaemonState, InstallCtx};

/// Test-binary pins backing the launchctl and LaunchAgents seams. Prod reads
/// them unconditionally (always `None` outside tests); setters are cfg(test).
#[cfg(test)]
static LAUNCHCTL_TEST_PIN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
#[cfg(test)]
static LAUNCH_AGENTS_TEST_PIN: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();

fn launchctl_cmd() -> Command {
    #[cfg(test)]
    if let Some(p) = LAUNCHCTL_TEST_PIN.get() {
        return Command::new(p);
    }
    Command::new("launchctl")
}

/// Probe the launchd job; `Some(RunningLaunchd)` when loaded, else `None`
/// so `daemon.rs` falls through to pidfile + addr probes.
pub(crate) fn detect_supervised(label: &str) -> Option<DaemonState> {
    let uid = current_uid();
    let out = launchctl_cmd()
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
    let out = launchctl_cmd()
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();
    let _ = out;
    let out = launchctl_cmd()
        .args([
            "bootstrap",
            &format!("gui/{uid}"),
            plist_path.to_str().unwrap_or_default(),
        ])
        .output()?;
    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        if !stderr.contains("already") && !stderr.contains("Bootstrap failed: 17") {
            anyhow::bail!("launchctl bootstrap failed: {stderr}");
        }
    }
    let out = launchctl_cmd()
        .args(["enable", &format!("gui/{uid}/{label}")])
        .output()?;
    if !out.status.success() {
        anyhow::bail!(
            "launchctl enable failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    let out = launchctl_cmd()
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
    if let Err(e) = install_logrotate_agent(&dir, &uid) {
        eprintln!("warning: com.toron.logrotate install failed (serve is already running): {e}");
    }
    Ok(())
}

/// launchd uninstall: bootout + remove the plist (idempotent).
pub(crate) fn uninstall_supervised(args: &DaemonArgs) -> anyhow::Result<()> {
    let label = args.label.clone();
    let uid = current_uid();
    let out = launchctl_cmd()
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

/// launchd start: install (write the plist + bootstrap) then start.
///
/// bd-j2vo: `kickstart` restarts the LOADED job, so a plist edited on disk
/// (the fd budget) never reached the running job — only a re-bootstrap
/// re-reads it. Installing first makes the plist the source of truth, and
/// the install's own kickstart starts the job.
pub(crate) fn start_supervised(args: &DaemonArgs, _pidfile: &Path) -> anyhow::Result<()> {
    crate::daemon::install(args)
}

/// launchd stop: bootout (+ port grace), pidfile kill, unmanaged/force
/// handling. Complete — `daemon.rs` does nothing after this returns.
pub(crate) fn stop_supervised(args: &DaemonArgs, pidfile: &Path) -> anyhow::Result<()> {
    use crate::daemon::{addr_alive, port_holder_pid, process_alive, read_pidfile};
    let label = args.label.clone();
    let uid = current_uid();
    let out = launchctl_cmd()
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();
    if let Ok(o) = out
        && o.status.success()
    {
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
	<!-- bd-j2vo: launchd's default soft maxfiles for a job is 256,
	     and the daemon holds one fd per wake subscriber. The daemon died of
	     EMFILE ("accept error: Too many open files") with 264 fds, which
	     stopped it accepting connections at all - the MCP endpoint went
	     unreachable and every commit was blocked behind it.

	     The key is SoftResourceLimits/HardResourceLimits -> NumberOfFiles:
	     launchd has no "LimitMaxFiles" key (that is systemd's name), so the
	     old plist declared a budget launchd silently ignored and the job ran
	     with the domain's 256. -->
	<key>SoftResourceLimits</key>
	<dict>
		<key>NumberOfFiles</key>
		<integer>4096</integer>
	</dict>
	<key>HardResourceLimits</key>
	<dict>
		<key>NumberOfFiles</key>
		<integer>4096</integer>
	</dict>
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

/// Hourly copy-truncate of the toron serve logs (bd-741i).
///
/// Weekly rotation left the serve logs at 1.3GB (`RUST_LOG=info` doorbell
/// poll-fail storm). Hourly + 1 MiB threshold caps both daemons. Pure —
/// golden-tested.
pub fn logrotate_plist_xml() -> String {
    let script = r#"for f in "$HOME/.local/state/toron/serve.out.log" "$HOME/.local/state/toron/serve.err.log"; do [ -f "$f" ] || continue; sz=$(stat -f%z "$f" 2>/dev/null || stat -c%s "$f"); if [ "$sz" -gt 1048576 ]; then cp "$f" "$f.1" && : > "$f"; fi; done"#;
    let script = script.replace('&', "&amp;");
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
    let _ = launchctl_cmd()
        .args(["bootout", &format!("gui/{uid}/{label}")])
        .output();
    let out = launchctl_cmd()
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
    #[cfg(test)]
    if let Some(p) = LAUNCH_AGENTS_TEST_PIN.get() {
        return Ok(p.clone());
    }
    let home = toron_core::config::home_env("HOME")
        .ok_or_else(|| anyhow::anyhow!("HOME unset; cannot locate LaunchAgents"))?;
    Ok(home.join("Library").join("LaunchAgents"))
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
            xml.contains("<key>SoftResourceLimits</key>")
                && xml.contains("<key>HardResourceLimits</key>")
                && xml.contains("<key>NumberOfFiles</key>")
                && xml.contains("<integer>4096</integer>"),
            "the unit must raise the fd budget with launchd's own key: launchd's \
             default is 256, the daemon holds one fd per subscriber, and \
             'LimitMaxFiles' is systemd's name — launchd ignores it"
        );
        assert!(
            !xml.contains("<key>LimitMaxFiles</key>"),
            "launchd has no LimitMaxFiles key — it is silently ignored, which is \
             how the job ran at 256 while the plist claimed 4096"
        );
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
    fn logrotate_plist_is_hourly_and_covers_serve_logs() {
        let xml = logrotate_plist_xml();
        assert!(xml.contains("<string>com.toron.logrotate</string>"));
        assert!(
            xml.contains("<key>StartInterval</key>\n\t<integer>3600</integer>"),
            "hourly, not weekly calendar"
        );
        assert!(!xml.contains("StartCalendarInterval"));
        assert!(xml.contains("serve.out.log"));
        assert!(xml.contains("1048576"), "1 MiB copy-truncate threshold");
    }

    #[test]
    fn logrotate_plist_is_valid_xml() {
        let xml = logrotate_plist_xml();
        let root = std::env::temp_dir().join(format!("toron-bd3pbzc-xml-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("com.toron.logrotate.plist");
        std::fs::write(&path, &xml).unwrap();
        let out = plutil_lint(&path);
        std::fs::remove_dir_all(&root).ok();
        assert!(
            out.status.success(),
            "plutil -lint must accept the generated plist: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn plutil_lint(path: &Path) -> std::process::Output {
        Command::new("plutil")
            .args(["-lint", path.to_str().unwrap()])
            .output()
            .unwrap()
    }

    #[test]
    fn logrotate_failure_cannot_take_serve_down() {
        use std::os::unix::fs::PermissionsExt;

        let root =
            std::env::temp_dir().join(format!("toron-bd3pbzc-install-{}", std::process::id()));
        let bin = root.join("bin");
        let agents = root.join("agents");
        std::fs::create_dir_all(&bin).unwrap();
        std::fs::create_dir_all(&agents).unwrap();
        let log = root.join("launchctl.log");
        let fake = bin.join("launchctl");
        std::fs::write(
            &fake,
            format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$*\" in\n  \
                 *com.toron.logrotate.plist*) echo 'Bootstrap failed: 5' >&2; exit 5 ;;\nesac\n",
                log.display()
            ),
        )
        .unwrap();
        std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
        let _ = LAUNCHCTL_TEST_PIN.set(fake);
        let _ = LAUNCH_AGENTS_TEST_PIN.set(agents.clone());

        let binary = Path::new("/usr/local/bin/toron");
        let key_path = Path::new("/Users/x/.config/toron/operator_nsec");
        let pass = Path::new("/Users/x/.config/toron/passphrase");
        let out_log = Path::new("/Users/x/.local/state/toron/serve.out.log");
        let err_log = Path::new("/Users/x/.local/state/toron/serve.err.log");
        let ctx = InstallCtx {
            label: "com.toron.serve",
            binary,
            addr: "127.0.0.1:18080",
            key_path,
            pass,
            out_log,
            err_log,
            state: Path::new("/Users/x/.local/state/toron"),
        };
        let result = install_supervised(&ctx);
        let calls = std::fs::read_to_string(&log).unwrap_or_default();

        assert!(
            result.is_ok(),
            "a side agent must never take serve down: {result:?}"
        );
        let serve = calls.find("com.toron.serve.plist").unwrap_or(usize::MAX);
        let rotate = calls
            .find("com.toron.logrotate.plist")
            .unwrap_or(usize::MAX);
        assert!(serve < rotate, "serve must be bootstrapped first: {calls}");
        assert!(
            calls.contains("kickstart"),
            "serve must be running before the side agent is touched: {calls}"
        );
        assert!(
            agents.join("com.toron.serve.plist").is_file(),
            "the serve plist must be installed even when logrotate fails: {calls}"
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
