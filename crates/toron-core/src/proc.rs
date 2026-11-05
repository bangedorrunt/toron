//! Bounded subprocess execution (bd-jvka).
//!
//! TWO bounds, because they are two DIFFERENT failures:
//!
//! 1. The CHILD can hang — it never exits. The wait needs a deadline.
//! 2. The DRAIN can hang even after the child is gone. A grandchild that
//!    inherited the pipe's write end keeps that end open, so the reader
//!    never sees EOF. An unbounded join then parks the caller FOREVER,
//!    with the child bound already satisfied — the shape that wedged the
//!    durable leaf watchdog and the pane dispatch.
//!
//! The child runs in its OWN process group, so the timeout kills the whole
//! group: a command's own children die with it instead of orphaning to
//! PPID 1.

use std::io::Read;
use std::process::{Command, Output, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long the pipe drain may take after the child exited.
///
/// The pipe holds at most one buffer (64 KiB) of unread output at that
/// point — the reader threads have been draining it while the child ran —
/// so this only ever has to cover the tail. It exists to make a stuck
/// grandchild a BOUNDED loss of output instead of an unbounded hang.
pub const DRAIN_GRACE: Duration = Duration::from_secs(5);

/// Why a bounded run did not produce output.
#[derive(Debug)]
pub enum ProcError {
    /// The child exceeded the bound and was killed with its group.
    Timeout { pid: u32 },
    /// The child could not be spawned, or the wait failed.
    Io(std::io::Error),
}

/// Run `cmd` to completion under a wall-clock bound.
///
/// The output is whatever the pipes carried; a drain that overruns
/// [`DRAIN_GRACE`] keeps the bytes read so far and detaches the reader
/// (the thread ends when the last writer closes the pipe).
pub fn run_bounded(cmd: &mut Command, timeout: Duration) -> Result<Output, ProcError> {
    use std::os::unix::process::CommandExt as _;
    let mut child = match cmd
        .process_group(0)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => return Err(ProcError::Io(e)),
    };
    let pid = child.id();
    let out = child.stdout.take().expect("piped stdout");
    let err = child.stderr.take().expect("piped stderr");
    let out_buf = Arc::new(Mutex::new(Vec::new()));
    let err_buf = Arc::new(Mutex::new(Vec::new()));
    let out_handle = spawn_reader(out, Arc::clone(&out_buf));
    let err_handle = spawn_reader(err, Arc::clone(&err_buf));

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(st)) => break st,
            Ok(None) => {
                if Instant::now() >= deadline {
                    kill_group(pid);
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(ProcError::Timeout { pid });
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(e) => {
                let _ = child.kill();
                return Err(ProcError::Io(e));
            }
        }
    };
    join_bounded(out_handle, Instant::now() + DRAIN_GRACE);
    join_bounded(err_handle, Instant::now() + DRAIN_GRACE);
    Ok(Output {
        status,
        stdout: take(&out_buf),
        stderr: take(&err_buf),
    })
}

/// Read the pipe to EOF into the shared buffer, so a partial read survives
/// an overrunning drain.
fn spawn_reader<R: Read + Send + 'static>(
    mut r: R,
    buf: Arc<Mutex<Vec<u8>>>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut chunk = [0u8; 8192];
        loop {
            match r.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut b) = buf.lock() {
                        b.extend_from_slice(&chunk[..n]);
                    }
                }
            }
        }
    })
}

/// Join the reader, but never past the deadline: past it the reader is
/// detached (it ends when the last writer closes the pipe) and the buffer
/// keeps whatever arrived.
fn join_bounded(handle: std::thread::JoinHandle<()>, deadline: Instant) {
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let _ = handle.join();
}

fn take(buf: &Arc<Mutex<Vec<u8>>>) -> Vec<u8> {
    buf.lock().map(|b| b.clone()).unwrap_or_default()
}

/// Kill the child's whole group. `kill -9` is unreliable across procps
/// (GS-025: watchdog tests saw a full 30s sleep), so the signal name is
/// explicit and the direct child kill follows it.
fn kill_group(pid: u32) {
    let _ = std::process::Command::new("kill")
        .args(["-s", "KILL", "--", &format!("-{pid}")])
        .status();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bd-jvka: a child that never exits is KILLED at the bound — the call
    /// returns a typed timeout instead of parking the caller.
    #[test]
    fn watchdog_kills_a_child_that_never_exits() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("sleep 30");
        let started = Instant::now();
        let err = run_bounded(&mut cmd, Duration::from_millis(300)).expect_err("must time out");
        assert!(matches!(err, ProcError::Timeout { .. }), "got {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the bound must fire, not the child: {:?}",
            started.elapsed()
        );
    }

    /// bd-jvka: THE bug — the child exits, but a grandchild it spawned keeps
    /// the pipe's write end open, so EOF never comes. The drain must be
    /// bounded: the call returns (with the child's own output) instead of
    /// hanging forever.
    #[test]
    fn watchdog_drain_is_bounded_when_a_grandchild_holds_the_pipe() {
        let mut cmd = Command::new("sh");
        cmd.arg("-c").arg("(sleep 30 &) ; printf 'child-done'");
        let started = Instant::now();
        let out = run_bounded(&mut cmd, Duration::from_secs(5)).expect("the child exits");
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(20),
            "the drain must be bounded, not parked: {elapsed:?}"
        );
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("child-done"),
            "the child's own output must survive: {:?}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
}
