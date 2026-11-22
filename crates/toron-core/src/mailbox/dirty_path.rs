// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! bd-nmilo: dirty-path evidence for lease liveness.
//!
//! A lease is a heartbeat, not a deed. Liveness extends it only while the
//! holder still has uncommitted changes in that path, or edited it within a
//! short grace window. Missing evidence falls back to TODAY's liveness rule —
//! never steal a file because we could not read git.

use std::path::Path;
use std::process::Command;
use std::time::Duration;

/// Two heartbeats (180 s x 2). Named constant; tests and the model use it.
pub const DIRTY_GRACE_SECS: i64 = 360;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathEvidence {
    Dirty { last_edit_ts_us: i64 },
    Clean { last_edit_ts_us: i64 },
    Unknown,
}

/// The seam. Production uses `GitDirtyPathSource`; tests inject a fake.
pub trait DirtyPathSource: Send + Sync {
    fn evidence(&self, repo_root: &Path, path: &str) -> PathEvidence;
}

/// Pure decision: the reducer half. No clock, process or filesystem here.
#[must_use]
pub fn should_extend(
    evidence: &PathEvidence,
    now_us: i64,
    grace_secs: i64,
    fallback_live: bool,
) -> bool {
    match evidence {
        PathEvidence::Dirty { .. } => true,
        PathEvidence::Clean { last_edit_ts_us } => {
            now_us.saturating_sub(*last_edit_ts_us) <= grace_secs.saturating_mul(1_000_000)
        }
        PathEvidence::Unknown => fallback_live,
    }
}

/// Production evidence: `git status --porcelain=v2 -z -- <path>` plus the
/// file's mtime. The call is bounded; a failure or timeout is `Unknown`,
/// never `Clean`.
pub struct GitDirtyPathSource {
    pub timeout: Duration,
}

impl Default for GitDirtyPathSource {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
        }
    }
}

impl DirtyPathSource for GitDirtyPathSource {
    fn evidence(&self, repo_root: &Path, path: &str) -> PathEvidence {
        let last_edit_ts_us = std::fs::metadata(repo_root.join(path))
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| (d.as_secs() as i64).saturating_mul(1_000_000))
            .unwrap_or(0);
        let child = match Command::new("git")
            .current_dir(repo_root)
            .args(["status", "--porcelain=v2", "-z", "--", path])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(c) => c,
            Err(_) => return PathEvidence::Unknown,
        };
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let _ = tx.send(child.wait_with_output());
        });
        match rx.recv_timeout(self.timeout) {
            Ok(Ok(out)) if out.status.success() => {
                if out.stdout.is_empty() {
                    PathEvidence::Clean { last_edit_ts_us }
                } else {
                    PathEvidence::Dirty { last_edit_ts_us }
                }
            }
            _ => PathEvidence::Unknown,
        }
    }
}

/// Always-Unknown source: the conservative fallback used when no repo can be
/// resolved. It never turns a missing fact into a steal.
pub struct UnknownDirtyPathSource;

impl DirtyPathSource for UnknownDirtyPathSource {
    fn evidence(&self, _repo_root: &Path, _path: &str) -> PathEvidence {
        PathEvidence::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_live_extends_and_unknown_falls_back() {
        assert!(should_extend(
            &PathEvidence::Dirty { last_edit_ts_us: 0 },
            10_000_000,
            DIRTY_GRACE_SECS,
            false
        ));
        assert!(!should_extend(
            &PathEvidence::Clean { last_edit_ts_us: 0 },
            DIRTY_GRACE_SECS * 1_000_000 + 1,
            DIRTY_GRACE_SECS,
            true
        ));
        assert!(should_extend(
            &PathEvidence::Clean {
                last_edit_ts_us: DIRTY_GRACE_SECS * 1_000_000 - 1
            },
            DIRTY_GRACE_SECS * 1_000_000,
            DIRTY_GRACE_SECS,
            false
        ));
        assert!(should_extend(
            &PathEvidence::Unknown,
            0,
            DIRTY_GRACE_SECS,
            true
        ));
        assert!(!should_extend(
            &PathEvidence::Unknown,
            0,
            DIRTY_GRACE_SECS,
            false
        ));
    }

    #[test]
    fn git_source_reads_a_real_temp_repo() {
        let root = std::env::temp_dir().join(format!(
            "toron-dirty-path-{}-{}",
            std::process::id(),
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir");
        let _ = Command::new("git")
            .current_dir(&root)
            .args(["init", "-q"])
            .status();
        std::fs::write(root.join("tracked.txt"), "one\n").expect("write");
        let src = GitDirtyPathSource::default();
        assert!(matches!(
            src.evidence(&root, "tracked.txt"),
            PathEvidence::Dirty { .. }
        ));
        let _ = Command::new("git")
            .current_dir(&root)
            .args(["add", "tracked.txt"])
            .status();
        let _ = Command::new("git")
            .current_dir(&root)
            .args([
                "-c",
                "user.name=T",
                "-c",
                "user.email=t@e",
                "commit",
                "-qm",
                "base",
            ])
            .status();
        assert!(matches!(
            src.evidence(&root, "tracked.txt"),
            PathEvidence::Clean { .. }
        ));
        let _ = std::fs::remove_dir_all(&root);
    }
}
