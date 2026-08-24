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
        anyhow::bail!(
            "another toron serve already holds {} (pid {pid}) — \
             one writer per data dir (bd-741i)",
            data_dir.display()
        );
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
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
