// governed-by: ADR-0003
//! Exclusive serve lock on the mailbox data dir (bd-741i).
//!
//! `toron serve` (HTTP or stdio) must be the only writer of a given data
//! dir. A second serve against the same root used to open its own SQLite
//! pool + mmap + 10Hz watch-style replay and grow to tens of GB RSS.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Held exclusive lock. Dropping removes the pid file if we still own it.
#[derive(Debug)]
pub struct ServeLock {
    path: PathBuf,
    pid: u32,
    /// The boot record beside the pid file: which artifact this process runs.
    record: PathBuf,
    _file: File,
}

impl Drop for ServeLock {
    fn drop(&mut self) {
        if let Ok(s) = fs::read_to_string(&self.path)
            && s.trim() == self.pid.to_string()
        {
            let _ = fs::remove_file(&self.path);
            toron_core::serve_build::remove(&self.record);
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
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)?;
    // The OS lock, not the pid text, is the authority: two starts racing on
    // an absent or stale file both parse nothing and both used to truncate
    // and write, so both "won". Hold the lock for the ServeLock lifetime.
    if let Err(error) = file.try_lock() {
        if !matches!(error, std::fs::TryLockError::WouldBlock) {
            anyhow::bail!(
                "could not take the serve lock on {}: {error:?}",
                path.display()
            );
        }
        let mut holder = String::new();
        let _ = file.read_to_string(&mut holder);
        let holder = holder.trim();
        anyhow::bail!(
            "another toron serve already holds {} (pid {}) — \
             one writer per data dir (bd-741i)",
            data_dir.display(),
            if holder.is_empty() { "unknown" } else { holder }
        );
    }
    let mut existing = String::new();
    if file.read_to_string(&mut existing).is_err() {
        existing.clear();
    }
    if let Ok(pid) = existing.trim().parse::<u32>()
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
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    write!(file, "{pid}")?;
    file.flush()?;

    let record = data_dir.join(toron_core::serve_build::SERVE_BUILD_FILE);
    if let Ok(exe) = std::env::current_exe()
        && let Some(snap) =
            toron_core::serve_build::snapshot_of(&exe, pid, toron_core::serve_build::now_unix())
        && let Err(e) = toron_core::serve_build::record(&record, &snap)
    {
        eprintln!(
            "toron: could not record the serving build ({e}) — \
             `toron daemon status` cannot name what this process runs"
        );
    }

    Ok(ServeLock {
        path,
        pid,
        record,
        _file: file,
    })
}

fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    Command::new("ps")
        .args(["-p", &pid.to_string()])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn pid_is_toron_serve(pid: u32) -> bool {
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

    /// Two `toron serve` starts racing on one data dir must not both win.
    /// The liveness check cannot decide this (both read no file or a stale
    /// one), so an OS lock on the pid file is the authority: even a pid file
    /// with no readable holder must refuse the second acquire.
    #[test]
    fn second_acquire_fails_even_when_the_pid_file_looks_stale() {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-lock-race-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let first = acquire(&dir).expect("first lock");
        // Blank the pid so the liveness check cannot be what rejects the
        // second acquire; only the held lock proves the dir is taken.
        fs::write(dir.join("serve.pid"), "").unwrap();
        let err = acquire(&dir).expect_err("a held lock must refuse a second serve");
        assert!(format!("{err:#}").contains("already holds"), "got: {err:#}");
        // Reuse-after-drop is covered by the live-pid test. Asserting it here
        // would race sibling tests that spawn children: a child can hold a
        // duplicate of the lock until it exits.
        drop(first);
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

    /// The boot record is what makes `daemon status` able to name a build at
    /// all: it must name THIS process and a real artifact, and it must go away
    /// with the lock so a later status cannot judge an earlier boot.
    #[test]
    fn acquire_records_the_booting_artifact_and_drop_clears_it() {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-lock-record-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let record = dir.join(toron_core::serve_build::SERVE_BUILD_FILE);
        assert!(
            toron_core::serve_build::read(&record).is_none(),
            "nothing to judge before a daemon boots"
        );

        let lock = acquire(&dir).expect("lock");
        let snap =
            toron_core::serve_build::read(&record).expect("the record names the booting build");
        assert_eq!(snap.pid, std::process::id());
        assert!(
            snap.binary.is_absolute(),
            "canonicalised so a later stat follows the same target: {}",
            snap.binary.display()
        );
        assert_eq!(
            snap.size,
            fs::metadata(&snap.binary).expect("stat artifact").len()
        );
        assert!(snap.started_at_unix > 0, "the boot time is recorded");

        assert_eq!(
            toron_core::serve_build::build_verdict(
                &snap,
                Some(snap.pid),
                toron_core::serve_build::artifact_id(&snap.binary)
            ),
            toron_core::serve_build::BuildVerdict::Current
        );

        drop(lock);
        assert!(
            toron_core::serve_build::read(&record).is_none(),
            "the record goes with the lock, so status cannot judge a boot that ended"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
