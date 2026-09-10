// governed-by: ADR-0003
//! Exclusive serve lock on the mailbox data dir (bd-741i).
//!
//! `toron serve` (HTTP or stdio) must be the only writer of a given data
//! dir. A second serve against the same root used to open its own SQLite
//! pool + mmap + 10Hz watch-style replay and grow to tens of GB RSS.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Held exclusive lock. Dropping removes the pid file if we still own it.
#[derive(Debug)]
pub struct ServeLock {
    path: PathBuf,
    pid: u32,
    _file: File,
}

impl Drop for ServeLock {
    fn drop(&mut self) {
        if let Ok(s) = fs::read_to_string(&self.path)
            && s.trim() == self.pid.to_string()
        {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Acquire an exclusive serve lock in `data_dir/serve.pid`.
///
/// Fails loud if another live process already holds it. A stale pid file
/// from a crash is replaced.
pub fn acquire(data_dir: &Path) -> anyhow::Result<ServeLock> {
    fs::create_dir_all(data_dir)?;
    let path = data_dir.join("serve.pid");
    if let Ok(s) = fs::read_to_string(&path)
        && let Ok(pid) = s.trim().parse::<u32>()
        && pid_alive(pid)
    {
        if pid_is_toron_serve(pid) {
            anyhow::bail!(
                "another toron serve already holds {} (pid {pid}) — \
                 one writer per data dir (bd-741i)",
                data_dir.display()
            );
        } else {
            eprintln!(
                "toron: stale serve.pid {pid} is not toron (reused by pid {}: {}) — taking over {} (bd-serve-pid-reuse-stale-lock-80by)",
                pid,
                String::from_utf8_lossy(
                    &Command::new("ps")
                        .args(["-o", "args=", "-p", &pid.to_string()])
                        .output()
                        .map(|o| o.stdout)
                        .unwrap_or_default()
                )
                .trim(),
                data_dir.display()
            );
        }
    }
    let pid = std::process::id();
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)?;
    write!(file, "{pid}")?;
    file.flush()?;
    Ok(ServeLock {
        path,
        pid,
        _file: file,
    })
}

fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    // Use `ps -p` rather than `kill -0` so EPERM (no permission to signal pid 1)
    // still counts as alive. `ps` succeeds for any existing pid.
    Command::new("ps")
        .args(["-p", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn pid_is_toron_serve(pid: u32) -> bool {
    // Check cmdline/args for "toron" to avoid PID-reuse false positives.
    // Broad check intentionally matches "toron", "toron_mcp", etc. for test
    // coverage (second_acquire test's holder is the test binary itself).
    // The /proc fast path lives in platform/<os>.rs (bd-nc5d).
    if crate::platform::proc_cmdline_contains(pid, "toron") {
        return true;
    }
    Command::new("ps")
        .args(["-o", "args=", "-p", &pid.to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .is_some_and(|args| args.contains("toron"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt as _;

    #[test]
    fn second_acquire_on_live_pid_fails() {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-lock-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let first = acquire(&dir).expect("first lock");
        let err = acquire(&dir).expect_err("second lock must fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("already holds") && msg.contains(&std::process::id().to_string()),
            "got: {msg}"
        );
        drop(first);
        acquire(&dir).expect("lock reusable after drop");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn stale_pid_is_replaced() {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-lock-stale-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("serve.pid"), "999999999").unwrap();
        acquire(&dir).expect("stale pid must not block");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn stale_pid_reuse_by_non_toron_is_taken_over() {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-lock-reuse-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // Spawn a non-toron process (sleep) to simulate PID reuse by a foreign binary
        let mut child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep");
        let pid = child.id();
        fs::write(dir.join("serve.pid"), pid.to_string()).unwrap();
        assert!(pid_alive(pid), "sleep pid must be alive");
        assert!(!pid_is_toron_serve(pid), "sleep must not be toron");
        acquire(&dir).expect("stale pid reuse by non-toron must not block");
        child.kill().ok();
        let _ = child.wait();
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn live_toron_pid_still_blocks() {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-lock-live-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        // Spawn a child whose argv[0] is "toron" to simulate a live
        // serve. Uses Command::arg0 (portable) — the old `sh -c "exec -a
        // toron …"` form depends on shell-specific exec -a semantics and
        // silently produced argv[0]=sh on runners whose /bin/sh lacks it
        // (GS-018: Ubuntu CI failed the detect assert).
        let mut child = std::process::Command::new("sleep")
            .arg0("toron")
            .arg("30")
            .spawn()
            .expect("spawn fake toron");
        let pid = child.id();
        fs::write(dir.join("serve.pid"), pid.to_string()).unwrap();
        assert!(pid_alive(pid));
        assert!(
            pid_is_toron_serve(pid),
            "fake toron pid must be detected as toron"
        );
        let err = acquire(&dir).expect_err("live toron pid must still block");
        assert!(format!("{err:#}").contains("already holds"));
        child.kill().ok();
        let _ = child.wait();
        let _ = fs::remove_dir_all(&dir);
    }
}
