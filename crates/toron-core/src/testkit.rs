// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Test-support shared by the workspace's harnesses.
//!
//! One type lives here because three crates spawn the same external server in
//! their tests: `toron-e2e` (via `tests/common`), `toron-workflow` (the cron
//! `spec_engine` fixture and `engine/tests.rs`), and `toron-mcp` (the live
//! engine gate). `toron-core` is the only crate all three depend on.

use std::path::Path;
use std::process::{Child, Command, Stdio};

/// bd-yolv: the spawn ledger. One JSON-ish line per live child, so a run that
/// is killed before `Drop` still leaves evidence of what it leaked.
///
/// This is the whole point: the 72 orphans carried no parent, no port, and no
/// test name, so the leak was undiagnosable and therefore unfixable. A leak you
/// cannot attribute is a leak you can only clean up by hand, forever.
fn ledger_path() -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "toron-resonate-children-{}.ledger",
        std::process::id()
    ))
}

fn record_spawn(pid: u32, port: u16) {
    if pid == 0 {
        return;
    }
    let line = format!("{pid} {port} {}\n", std::process::id());
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(ledger_path())
    {
        use std::io::Write;
        let _ = f.write_all(line.as_bytes());
    }
}

fn forget_spawn(pid: u32) {
    let path = ledger_path();
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return;
    };
    let kept: Vec<&str> = raw
        .lines()
        .filter(|l| l.split_whitespace().next() != Some(pid.to_string().as_str()))
        .collect();
    let _ = std::fs::write(&path, kept.join("\n"));
}

/// bd-yolv: kill anything the LAST run left behind, and only that.
///
/// The identity check is the safety property and it is not optional: a pid is
/// recycled by the OS, so killing a bare recorded pid could one day take out an
/// unrelated process — including a real daemon. So a recorded pid is killed only
/// when the live process at that pid is still a `resonate serve` carrying the
/// port we recorded. Anything else is left strictly alone.
pub fn reap_orphaned_servers() -> usize {
    let path = ledger_path();
    let Ok(raw) = std::fs::read_to_string(&path) else {
        return 0;
    };
    let mut reaped = 0;
    let mut survivors: Vec<String> = Vec::new();
    for line in raw.lines() {
        let mut it = line.split_whitespace();
        let (Some(pid), Some(port)) = (it.next(), it.next()) else {
            continue;
        };
        let (Ok(pid), Ok(port)) = (pid.parse::<u32>(), port.parse::<u16>()) else {
            continue;
        };
        if !pid_alive_and_is_our_resonate(pid, port) {
            // Not ours any more: either already gone, or the pid was recycled by
            // something that is NOT our resonate server. Forget it either way —
            // a stale ledger line must never accumulate.
            continue;
        }
        unsafe_free_signal(pid, TERM);
        reaped += 1;
    }
    let _ = std::fs::write(&path, survivors.join("\n"));
    if reaped > 0 {
        eprintln!(
            "toron testkit: reaped {reaped} orphaned resonate serve process(es) from a previous run"
        );
    }
    reaped
}

/// Is the live process at `pid` still our `resonate serve` on `port`?
///
/// Two independent facts, because either alone can lie: the command line proves
/// it is the right PROGRAM, and the recorded port proves it is the right
/// INSTANCE. A recycled pid running something else fails the first; a different
/// resonate on another port fails the second.
fn pid_alive_and_is_our_resonate(pid: u32, port: u16) -> bool {
    let Ok(out) = std::process::Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "command="])
        .output()
    else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    identity_matches(&String::from_utf8_lossy(&out.stdout), port)
}

/// The pure identity core, split out so a fence can falsify it.
///
/// bd-yolv: my first version of the fence used a `sleep` stand-in, and it
/// passed whether or not this function checked the PORT — because `sleep`
/// already fails the program check, so the port check was never exercised. A
/// green fence that cannot go red is worse than none, since it reads as
/// coverage. Taking the cmdline as a parameter makes the port half testable
/// with a plain string.
#[must_use]
pub fn identity_matches(cmdline: &str, port: u16) -> bool {
    if !cmdline.contains("resonate") {
        return false;
    }
    // The port must match as a WHOLE TOKEN. My first version used
    // `contains(&format!("--server-port {port}"))`, and my own fence caught it:
    // "--server-port 5555" contains "--server-port 55", so a recorded port 55
    // would have matched — and killed — a server on 5555. A substring check on
    // a number is a substring check on the wrong thing.
    let want = format!("--server-port {port}");
    cmdline
        .split_whitespace()
        .collect::<Vec<_>>()
        .windows(2)
        .any(|w| w[0] == "--server-port" && w[1] == port.to_string())
        || cmdline.split_whitespace().any(|t| t == want)
}

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
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}
#[cfg(not(unix))]
const fn unsafe_free_signal(_pid: u32, _sig: i32) {}

/// SIGTERM.
const TERM: i32 = 15;

/// How long a child gets to exit on SIGTERM before SIGKILL.
///
/// bd-yolv: this MUST exceed the server's own graceful-shutdown window or the
/// SIGTERM is theatre. `resonate serve` ships `--server-shutdown-timeout` with a
/// **10s** default, so a 2s grace would have SIGKILLed the server mid-shutdown —
/// the exact outcome the SIGTERM was added to avoid, just slower to arrive. 12s
/// clears the default with margin.
///
/// The wait itself is not a cost: the loop returns the moment the child exits,
/// so a well-behaved child still costs ~0ms. Only a WEDGED child pays the full
/// grace, which is the case worth paying for.
const KILL_GRACE: std::time::Duration = std::time::Duration::from_millis(12_000);

/// A spawned child process that is killed and reaped when its scope ends.
///
/// `std::process::Child` has no `Drop`. A harness that spawns a server and
/// releases it with a trailing `kill()` therefore leaks the process on every
/// path that skips that statement — a failing assertion, a panic, or a test
/// binary cancelled mid-run. The orphan is reparented to init, keeps its
/// sqlite file and its port, and nothing ever reaps it: 54 survivors holding
/// 738 MB were measured on one host, every one of them `ppid=1`.
///
/// Making the scope own the child releases it on the normal path, the panic
/// path, and cancellation alike. `Child::kill` sends SIGKILL, which a child
/// cannot ignore, and the following `wait` reaps the zombie — so a release is
/// final rather than best-effort, and a test can assert on it.
///
/// Durable state is unaffected: the servers keep it on disk, so killing the
/// process at scope end loses nothing a restart row depends on.
#[derive(Debug)]
pub struct ChildGuard(Option<Child>);

impl ChildGuard {
    /// Take ownership of a spawned child's lifetime.
    pub fn spawn(cmd: &mut Command) -> std::io::Result<Self> {
        Ok(Self(Some(cmd.spawn()?)))
    }

    /// Spawn `resonate serve` (sqlite in `dir`, loopback only) as a guarded
    /// child. Returns `None` when the `resonate` binary is absent, which is an
    /// environmental skip on a runner without the stack, never a product
    /// failure.
    ///
    /// bd-yolv: this is also where a PREVIOUS run's orphans get reaped. 72
    /// `resonate serve` processes (948 MB, every one `ppid=1`) were measured on
    /// one host. `ChildGuard` is correct — it has a real `Drop` — so the orphans
    /// came from a run that was killed before `Drop` could run, and a
    /// reparented process leaves no record of what it was. So this one records
    /// what it spawns, and the next run cleans up after the last one.
    pub fn resonate_serve(port: u16, dir: &Path) -> std::io::Result<Option<Self>> {
        if !resonate_installed() {
            return Ok(None);
        }
        reap_orphaned_servers();
        let mut cmd = Command::new("resonate");
        cmd.args([
            "serve",
            "--storage-type",
            "sqlite",
            "--server-port",
            &port.to_string(),
            "--server-host",
            "127.0.0.1",
        ])
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null());
        let guard = Self::spawn(&mut cmd)?;
        record_spawn(guard.pid().unwrap_or(0), port);
        Ok(Some(guard))
    }

    /// The child's pid, for a caller that must report or probe it.
    pub fn pid(&self) -> Option<u32> {
        self.0.as_ref().map(Child::id)
    }

    /// Release the child now instead of at scope end. Idempotent.
    ///
    /// bd-yolv: SIGTERM first, SIGKILL only as the backstop. `resonate serve`
    /// ships `--server-shutdown-timeout` (default 10s), so it has a real
    /// graceful-shutdown path — and `Child::kill()` is SIGKILL, which skips all
    /// of it. A killed server leaves its sqlite file mid-write and never runs
    /// whatever cleanup it does on the way out. The escalation is BOUNDED, so a
    /// server wedged in shutdown cannot hang a test past [`KILL_GRACE`].
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
            forget_spawn(pid);
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Whether the `resonate` binary is on PATH and runnable.
pub fn resonate_installed() -> bool {
    Command::new("resonate")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A scoped long-lived child, used as a stand-in for the resonate server
    /// so the fence does not need the binary.
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

    /// bd-yolv: a recorded pid is killed ONLY when the live process at that
    /// pid is still our `resonate serve` on the port we recorded.
    ///
    /// This is the safety property of the whole reaper, and it is the reason a
    /// bare "kill the recorded pid" would be unacceptable: pids are RECYCLED, so
    /// a ledger line read after the original died can name an unrelated process.
    /// One that happens to be a real daemon. A recycled pid running something
    /// else must fail the program check; a different resonate on another port
    /// must fail the instance check.
    #[test]
    fn the_reaper_refuses_a_pid_that_is_not_our_resonate() {
        // This very process is alive, is not resonate, and answers on no port.
        let me = std::process::id();
        assert!(
            !pid_alive_and_is_our_resonate(me, 1),
            "the reaper would signal a live non-resonate process ({me})"
        );
        // A pid that cannot exist fails closed rather than defaulting to "kill".
        assert!(
            !pid_alive_and_is_our_resonate(u32::MAX, 1),
            "an impossible pid must fail closed"
        );
    }

    /// The port is the instance identity. A real `resonate serve` pid paired
    /// with the WRONG port is a different instance and must be left alone —
    /// otherwise the reaper could kill a live server a concurrent test is using.
    #[test]
    fn the_reaper_checks_the_instance_not_just_the_program() {
        // `sleep` stands in for any long-lived process: alive, wrong program.
        let mut child = Command::new("sleep")
            .arg("30")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn stand-in");
        let pid = child.id();
        assert!(
            !pid_alive_and_is_our_resonate(pid, 1),
            "pid {pid} is not resonate"
        );
        let _ = child.kill();
        let _ = child.wait();
    }

    /// bd-yolv: a ledger line for a dead child must be FORGOTTEN, not kept.
    /// A registry that only grows is the same unbounded-artifact defect as the
    /// journal and audit files: it would accumulate one line per test run
    /// forever, and a stale line is a future false positive.
    #[test]
    fn a_released_child_is_forgotten_and_a_dead_one_leaves_no_line() {
        let _ = std::fs::remove_file(ledger_path());
        record_spawn(4242, 5555);
        assert!(
            std::fs::read_to_string(ledger_path())
                .unwrap()
                .contains("4242")
        );
        forget_spawn(4242);
        let after = std::fs::read_to_string(ledger_path()).unwrap_or_default();
        assert!(
            !after.contains("4242"),
            "a released child must leave no line: {after:?}"
        );
        let _ = std::fs::remove_file(ledger_path());
    }

    /// Reaping must be a no-op, not a crash, when there is no ledger at all —
    /// which is the state of every host that has never leaked.
    #[test]
    fn reaping_with_no_ledger_is_a_clean_no_op() {
        let _ = std::fs::remove_file(ledger_path());
        assert_eq!(
            reap_orphaned_servers(),
            0,
            "nothing recorded, nothing to reap"
        );
    }

    /// The recorded pid of 0 is meaningless and must never be signalled.
    #[test]
    fn a_zero_pid_is_never_recorded() {
        let _ = std::fs::remove_file(ledger_path());
        record_spawn(0, 1);
        assert!(
            !std::fs::read_to_string(ledger_path())
                .unwrap_or_default()
                .contains("0 1"),
            "pid 0 names the process group, not a child; recording it would be a footgun"
        );
    }

    /// bd-yolv: the port half of the identity check, which the `sleep`
    /// stand-in could never reach. A live `resonate serve` on a DIFFERENT port
    /// is a different instance — most likely a concurrent test's server — and
    /// killing it would break a test that is passing.
    #[test]
    fn the_port_half_of_the_identity_check_is_falsifiable() {
        let ours =
            "resonate serve --storage-type sqlite --server-port 5555 --server-host 127.0.0.1";
        let other =
            "resonate serve --storage-type sqlite --server-port 6666 --server-host 127.0.0.1";
        assert!(identity_matches(ours, 5555), "our own instance must match");
        assert!(
            !identity_matches(other, 5555),
            "a resonate on another port is a DIFFERENT instance; the reaper must leave it alone"
        );
        assert!(!identity_matches(ours, 6666), "and the check is symmetric");
        assert!(
            !identity_matches("/usr/bin/sleep 30", 5555),
            "wrong program"
        );
        assert!(
            !identity_matches("", 5555),
            "an unreadable cmdline fails closed"
        );
        // The port must match as a WHOLE number: a recorded 555 must not be
        // satisfied by a server on 5555, which is a different instance.
        assert!(
            !identity_matches("resonate serve --server-port 5555", 55),
            "a port prefix must not satisfy the check: 55 is not 5555"
        );
    }

    /// bd-yolv: the release is SIGTERM first, so the server's own
    /// `--server-shutdown-timeout` path actually runs. `Child::kill()` is
    /// SIGKILL and skips all of it, leaving the sqlite file mid-write.
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
    /// worth verifying rather than arguing.
    ///
    /// `resonate serve` drains for `--server-shutdown-timeout` (10s default), so
    /// a grace below that SIGKILLs the server mid-drain — the exact outcome the
    /// SIGTERM exists to prevent. The stand-in here traps SIGTERM and then takes
    /// `SLOW_SHUTDOWN` to finish, so a too-short grace kills it before it can
    /// record that it shut down cleanly.
    #[cfg(unix)]
    #[test]
    fn the_grace_outlasts_a_slow_shutdown() {
        const SLOW_SHUTDOWN: u64 = 2;
        assert!(
            KILL_GRACE > std::time::Duration::from_secs(SLOW_SHUTDOWN),
            "grace {KILL_GRACE:?} is shorter than a {SLOW_SHUTDOWN}s shutdown, so a server \
             that needs its full drain window is SIGKILLed mid-shutdown"
        );
        assert!(
            KILL_GRACE >= std::time::Duration::from_secs(10),
            "resonate's own --server-shutdown-timeout defaults to 10s; a grace under that \
             makes the SIGTERM pointless"
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
