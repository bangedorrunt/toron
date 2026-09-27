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
    pub fn resonate_serve(port: u16, dir: &Path) -> std::io::Result<Option<Self>> {
        if !resonate_installed() {
            return Ok(None);
        }
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
        Self::spawn(&mut cmd).map(Some)
    }

    /// The child's pid, for a caller that must report or probe it.
    pub fn pid(&self) -> Option<u32> {
        self.0.as_ref().map(Child::id)
    }

    /// Release the child now instead of at scope end. Idempotent.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.0.take() {
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
