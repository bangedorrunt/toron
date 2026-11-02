// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Test-support shared by the workspace's harnesses.
//!
//! Generic child management (`ChildGuard` scope/drop/kill, pid aliveness)
//! plus the `toron`-binary spawn harness live here — `toron-core` is the
//! only crate every test harness depends on.
//!
use std::process::{Child, Command, Stdio};

/// bd-yolv: SIGTERM, sent by number because `std` exposes no portable
/// `kill(2)` wrapper and this crate is `#![forbid(unsafe_code)]` — the whole
/// reason the belt-and-braces `PR_SET_PDEATHSIG` fix is unavailable here.
///
/// On Unix this is `kill(pid, SIGTERM)`. Off Unix there is no equivalent, so it
/// is a no-op and [`ChildGuard::kill`] falls straight through to SIGKILL, which
/// is the pre-existing behaviour rather than a new gap.
#[cfg(unix)]
fn unsafe_free_signal(pid: u32, sig: i32) {
    // `kill` is async-signal-safe and takes only a pid and an int, so a shell
    // out is correct and keeps the crate free of `unsafe`.
    let _ = std::process::Command::new("kill")
        .arg(format!("-{sig}"))
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}
#[cfg(not(unix))]
const fn unsafe_free_signal(_pid: u32, _sig: i32) {}

/// SIGTERM.
const TERM: i32 = 15;

/// How long a child gets to exit on SIGTERM before SIGKILL.
///
/// bd-yolv: this MUST exceed a slow child's own drain window or the SIGTERM
/// is theatre. 12s clears slow shutdowns with margin.
///
/// The wait itself is not a cost: the loop returns the moment the child exits,
/// so a well-behaved child still costs ~0ms. Only a WEDGED child pays the full
/// grace, which is the case worth paying for.
const KILL_GRACE: std::time::Duration = std::time::Duration::from_millis(12_000);

/// A spawned child process that is killed and reaped when its scope ends.
///
/// `std::process::Child` has no `Drop`. A harness that spawns a child and
/// releases it with a trailing `kill()` therefore leaks the process on every
/// path that skips that statement — a failing assertion, a panic, or a test
/// binary cancelled mid-run. The orphan is reparented to init and nothing
/// ever reaps it.
///
/// Making the scope own the child releases it on the normal path, the panic
/// path, and cancellation alike. `Child::kill` sends SIGKILL, which a child
/// cannot ignore, and the following `wait` reaps the zombie — so a release is
/// final rather than best-effort, and a test can assert on it.
#[derive(Debug)]
pub struct ChildGuard(Option<Child>);

impl ChildGuard {
    /// Take ownership of a spawned child's lifetime.
    pub fn spawn(cmd: &mut Command) -> std::io::Result<Self> {
        Ok(Self(Some(cmd.spawn()?)))
    }

    /// The child's pid, for a caller that must report or probe it.
    pub fn pid(&self) -> Option<u32> {
        self.0.as_ref().map(Child::id)
    }

    /// Release the child now instead of at scope end. Idempotent.
    ///
    /// bd-yolv: SIGTERM first, SIGKILL only as the backstop. `Child::kill()`
    /// is SIGKILL, which skips any graceful-shutdown path the child has — a
    /// killed child leaves its files mid-write and never runs cleanup. The
    /// escalation is BOUNDED, so a child wedged in shutdown cannot hang a
    /// test past [`KILL_GRACE`].
    pub fn kill(&mut self) {
        if let Some(mut child) = self.0.take() {
            // SAFETY of the wait: `try_wait` in a bounded loop never blocks, so
            // this cannot deadlock even if the child ignores SIGTERM.
            let pid = child.id();
            unsafe_free_signal(pid, TERM);
            let deadline = std::time::Instant::now() + KILL_GRACE;
            loop {
                match child.try_wait() {
                    Ok(Some(_)) => return,
                    Ok(None) if std::time::Instant::now() < deadline => {
                        std::thread::sleep(std::time::Duration::from_millis(20));
                    }
                    // Exited, or out of grace: SIGKILL is the backstop either way.
                    _ => break,
                }
            }
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Whether a pid names a process this host still knows about.
///
/// Used by the harnesses' own fences to assert a release actually happened.
/// `ps` is the portable, unsafe-free probe: it exits non-zero for a pid that
/// is gone or a zombie that has been reaped.
pub fn pid_alive(pid: u32) -> bool {
    Command::new("ps")
        .args(["-p", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// The spawn harness: every `toron` a test runs is built here.
// ---------------------------------------------------------------------------

/// This test process's sandbox: the directory its own in-process code already
/// uses when it is under test (`$TMPDIR/toron-test-root-<pid>`, or the
/// directory its own harness marker names).
///
/// A spawned child gets the SAME directory, so a test and the `toron` it runs
/// agree on the data root, the config home and the state home (`wake.sock`)
/// by construction instead of by inheriting the operator's.
pub fn harness_root() -> std::path::PathBuf {
    crate::config::ambient_sandbox()
        .map(|sandbox| sandbox.root)
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("toron-test-root-{}", std::process::id()))
        })
}

/// Put `cmd` under test: the marker ([`crate::config::UNDER_TEST_ENV`]), an
/// explicit `TORON_DATA_DIR`, and a config home and state home, all inside
/// [`harness_root`]. `TORON_MCP_URL` is dropped so no ambient daemon is used.
///
/// Apply it to every command that runs `toron`, directly or through a hook
/// (a `git commit` in a repo with toron's hooks installed runs `toron`). A
/// test that needs a different root sets its own variable AFTER this call;
/// the later `env` wins, and the marker still keeps the child off the
/// operator's home for everything it does not pin.
pub fn apply_test_env(cmd: &mut Command) -> &mut Command {
    sandbox_env(cmd).env_remove("TORON_MCP_URL")
}

/// The sandbox half of [`apply_test_env`], without touching `TORON_MCP_URL`
/// (an `env_clear`ed command has nothing to remove, and a recorded removal
/// still shows up in `Command::get_envs`).
fn sandbox_env(cmd: &mut Command) -> &mut Command {
    let root = harness_root();
    let home = root.join("home");
    for dir in [
        root.clone(),
        home.join(".config"),
        home.join(".local").join("state"),
    ] {
        let _ = std::fs::create_dir_all(dir);
    }
    cmd.env(crate::config::UNDER_TEST_ENV, &root)
        .env("TORON_DATA_DIR", &root)
        .env("XDG_CONFIG_HOME", home.join(".config"))
        .env("XDG_STATE_HOME", home.join(".local").join("state"))
}

/// A `toron` command under test (see [`apply_test_env`]). The ONE way a test
/// spawns the binary: `tests/spawn_harness.rs` in toron-cli fails the build
/// for any spawn site that bypasses it.
pub fn toron_command(bin: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut cmd = Command::new(bin);
    apply_test_env(&mut cmd);
    cmd
}

/// [`toron_command`] with the environment cleared first. Only `PATH` and
/// `TMPDIR` are kept (the child must agree with this process on the temp root).
pub fn toron_command_clean(bin: impl AsRef<std::ffi::OsStr>) -> Command {
    let mut cmd = Command::new(bin);
    cmd.env_clear();
    for keep in ["PATH", "TMPDIR"] {
        if let Some(value) = std::env::var_os(keep) {
            cmd.env(keep, value);
        }
    }
    sandbox_env(&mut cmd);
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A scoped long-lived child, used as a release-assertion stand-in.
    fn sleeper() -> ChildGuard {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "sleep 300"]);
        ChildGuard::spawn(&mut cmd).expect("spawn sleeper")
    }

    #[test]
    fn scope_end_releases_the_child() {
        let pid = {
            let guard = sleeper();
            let pid = guard.pid().expect("pid");
            assert!(pid_alive(pid), "child is running inside its scope");
            pid
            // guard drops here
        };
        assert!(
            !pid_alive(pid),
            "the child outlived its scope — a bare Child would do exactly this"
        );
    }

    #[test]
    fn a_panicking_scope_still_releases_the_child() {
        let pid = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let seen = std::sync::Arc::clone(&pid);
        let result = std::panic::catch_unwind(move || {
            let guard = sleeper();
            seen.store(
                guard.pid().expect("pid"),
                std::sync::atomic::Ordering::SeqCst,
            );
            panic!("assertion failed mid-test");
        });
        assert!(result.is_err(), "the scope really did panic");
        let pid = pid.load(std::sync::atomic::Ordering::SeqCst);
        assert!(pid != 0, "the child was spawned before the panic");
        assert!(
            !pid_alive(pid),
            "a panicking assertion leaked the child — this is the bd-6h9l orphan"
        );
    }

    #[test]
    fn release_is_idempotent() {
        let mut guard = sleeper();
        let pid = guard.pid().expect("pid");
        guard.kill();
        guard.kill();
        assert!(!pid_alive(pid), "released once and only once");
    }
}

#[cfg(test)]
mod bd_yolv_tests {
    use super::*;

    /// bd-yolv: the release is SIGTERM first, so a child's own shutdown path
    /// actually runs. `Child::kill()` is SIGKILL and skips all of it.
    ///
    /// Observed, not asserted: a shell child traps SIGTERM and records that it
    /// arrived. A SIGKILL-only release leaves no such record, because the
    /// process dies before any trap can run.
    #[cfg(unix)]
    #[test]
    fn the_release_is_sigterm_before_sigkill() {
        let dir = std::env::temp_dir().join(format!("toron-term-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let marker = dir.join("caught");
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(format!(
                // `sleep 30 & wait`, NOT a bare `sleep 30`: a shell DEFERS trap
                // handling while blocked on a foreground child, so a bare sleep
                // would swallow the SIGTERM until it finished 30s later and the
                // fence would report a broken signal path that does not exist.
                "trap 'echo caught > {}; exit 0' TERM; sleep 30 & wait",
                marker.display()
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut guard = ChildGuard::spawn(&mut cmd).expect("spawn");
        let pid = guard.pid().expect("pid");
        // Give the trap time to install before signalling.
        std::thread::sleep(std::time::Duration::from_millis(300));
        guard.kill();
        assert!(
            marker.exists(),
            "the child never saw SIGTERM (pid {pid}): a SIGKILL-only release cannot run a \
             trap, so this is how the graceful path is observed rather than assumed"
        );
        // And it is really gone, not merely signalled.
        assert!(!pid_is_alive(pid), "pid {pid} survived the release");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn pid_is_alive(pid: u32) -> bool {
        std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }

    /// bd-yolv: the GRACE has to outlast a slow shutdown, and that is a number
    /// worth verifying rather than arguing. A grace below a child's drain
    /// window SIGKILLs it mid-drain — the exact outcome the SIGTERM exists
    /// to prevent. The stand-in here traps SIGTERM and then takes
    /// `SLOW_SHUTDOWN` to finish, so a too-short grace kills it before it can
    /// record that it shut down cleanly.
    #[cfg(unix)]
    #[test]
    fn the_grace_outlasts_a_slow_shutdown() {
        const SLOW_SHUTDOWN: u64 = 2;
        assert!(
            KILL_GRACE > std::time::Duration::from_secs(SLOW_SHUTDOWN),
            "grace {KILL_GRACE:?} is shorter than a {SLOW_SHUTDOWN}s shutdown, so a child \
             that needs its full drain window is SIGKILLed mid-shutdown"
        );
        assert!(
            KILL_GRACE >= std::time::Duration::from_secs(10),
            "a grace under 10s makes the SIGTERM pointless against slow drains"
        );

        let dir = std::env::temp_dir().join(format!("toron-slow-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("dir");
        let marker = dir.join("drained");
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(format!(
                "trap 'sleep {SLOW_SHUTDOWN}; echo drained > {}; exit 0' TERM; sleep 30 & wait",
                marker.display()
            ))
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut guard = ChildGuard::spawn(&mut cmd).expect("spawn");
        let pid = guard.pid().expect("pid");
        std::thread::sleep(std::time::Duration::from_millis(300));
        let t0 = std::time::Instant::now();
        guard.kill();
        let waited = t0.elapsed();
        assert!(
            marker.exists(),
            "the child was killed before it could finish draining (waited {waited:?}): \
             KILL_GRACE={KILL_GRACE:?} does not outlast the shutdown"
        );
        assert!(!pid_is_alive(pid), "pid {pid} survived");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
