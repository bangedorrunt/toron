// governed-by: ADR-0003, ADR-0009
//! Which build is the daemon actually running?
//!
//! A daemon started from `target/debug/toron` keeps executing the file it was
//! exec'd from even after a rebuild replaces it, so a fix can be built,
//! installed, and still not be live. Finding that out used to mean `ps` plus
//! `stat` on the right path (2026-09-30: a daemon an hour behind its own fix
//! rewrote the operator key on every boot while `doctor` reported all-green).
//!
//! So `toron serve` leaves a boot record beside its `serve.pid`, and both
//! `toron daemon status` and `toron doctor` read it back.
//!
//! Two times and one identity, deliberately kept apart:
//!
//! - `started_at_unix` and `built_at_unix` are REPORTED — they are what a human
//!   needs to see ("booted 21:12:50 from a build written 20:29:17").
//! - The VERDICT never compares the two clocks. It compares the artifact's
//!   identity before and after, because clock comparison has no answer for a
//!   rebuild that lands in the same second, and it invents a false "stale" when
//!   a checkout or a `touch` rewrites the file's time without changing a byte.
//!   A rebuild writes a NEW file and renames it over the old one, so the inode
//!   changes even when the size does not; `(inode, size)` is what survives that.
//!
//! The verdict is a pure function of three inputs so it is testable without a
//! daemon, a clock, or a filesystem.

use std::path::{Path, PathBuf};

/// Boot record dropped beside `serve.pid` in the mailbox data root.
pub const SERVE_BUILD_FILE: &str = "serve.build.json";

/// What a booted process can say about the artifact it is executing: which
/// file, when it began, when that file was last written, and how to recognise
/// the file again.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BuildSnapshot {
    /// The pid that took the serve lock.
    pub pid: u32,
    /// The artifact path, canonicalised at boot so a later stat follows the
    /// same target even if the invoking path was a symlink.
    pub binary: PathBuf,
    /// Unix seconds at which this process reached its lock — its own start.
    pub started_at_unix: i64,
    /// Unix seconds of the artifact's last write — its build time.
    pub built_at_unix: i64,
    /// Filesystem identity of the artifact: a rebuild swaps the inode.
    pub ino: u64,
    /// Byte length, which catches a same-inode rewrite on filesystems that
    /// edit in place.
    pub size: u64,
}

/// Cheap identity of a file, safe to re-stat on every status call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub struct ArtifactId {
    pub ino: u64,
    pub size: u64,
}

/// Whether the daemon that is serving now booted from the artifact on disk now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BuildVerdict {
    /// A different process (or none) holds the daemon address, so the record
    /// describes an earlier boot and cannot be judged.
    NotServing,
    /// The served artifact is byte-for-byte the one the daemon booted from.
    Current,
    /// The artifact was rebuilt, replaced or removed since the daemon booted,
    /// so the running daemon is NOT the build on disk.
    Replaced,
}

impl BuildSnapshot {
    /// The identity to re-check on disk.
    pub fn artifact(&self) -> ArtifactId {
        ArtifactId {
            ino: self.ino,
            size: self.size,
        }
    }
}

impl BuildVerdict {
    /// True only for the case that costs you a debug cycle: a live daemon
    /// serving a build that is no longer the build on disk.
    pub fn is_replaced(self) -> bool {
        matches!(self, Self::Replaced)
    }
}

/// The verdict. `serving_pid` is whoever holds the daemon address right now,
/// `on_disk` is the current identity of `snap.binary` (`None` when the file is
/// gone). Pure: no clock, no filesystem, no environment.
pub fn build_verdict(
    snap: &BuildSnapshot,
    serving_pid: Option<u32>,
    on_disk: Option<ArtifactId>,
) -> BuildVerdict {
    // Whoever holds the address now is the only process this record can
    // describe; anything else is a leftover from an earlier boot.
    if serving_pid != Some(snap.pid) {
        return BuildVerdict::NotServing;
    }
    match on_disk {
        // Identity match, not clock order: the same artifact is still there.
        Some(id) if id == snap.artifact() => BuildVerdict::Current,
        // Rebuilt, swapped, resized, or gone — all mean the daemon being served
        // is not the build on disk.
        _ => BuildVerdict::Replaced,
    }
}

/// Identity of `path`, or `None` when it cannot be stat'd.
pub fn artifact_id(path: &Path) -> Option<ArtifactId> {
    let md = std::fs::metadata(path).ok()?;
    Some(ArtifactId {
        ino: ino_of(&md),
        size: md.len(),
    })
}

/// When the artifact at `path` was written, in unix seconds.
///
/// Deliberately separate from [`ArtifactId`]: this is the REPORTED build time,
/// never part of the verdict. A `touch` moves the time without changing the
/// artifact, and a rebuild can land in the same second as the boot, so the two
/// must not be conflated in either direction.
pub fn artifact_mtime_unix(path: &Path) -> Option<i64> {
    std::fs::metadata(path)
        .ok()?
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_secs() as i64)
}

/// Describe the artifact at `binary` as booted by `pid` at `started_at_unix`.
/// `None` when the artifact cannot be stat'd — a record we cannot verify is
/// worse than no record, so the caller must not write one.
pub fn snapshot_of(binary: &Path, pid: u32, started_at_unix: i64) -> Option<BuildSnapshot> {
    let md = std::fs::metadata(binary).ok()?;
    let built_at_unix = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    Some(BuildSnapshot {
        pid,
        binary: std::fs::canonicalize(binary).unwrap_or_else(|_| binary.to_path_buf()),
        started_at_unix,
        built_at_unix,
        ino: ino_of(&md),
        size: md.len(),
    })
}

/// Write the boot record. Tolerant by design: a daemon that cannot record its
/// build must still start (the record is a diagnostic, never a gate).
pub fn record(path: &Path, snap: &BuildSnapshot) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec(snap)?)
}

/// Read the boot record. `None` for absent, unreadable or malformed — a
/// half-written record must read as "no record", never as a false verdict.
pub fn read(path: &Path) -> Option<BuildSnapshot> {
    let raw = std::fs::read(path).ok()?;
    serde_json::from_slice(&raw).ok()
}

/// Remove the boot record (best effort), used when the lock is released.
pub fn remove(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// Unix seconds now. The only clock read in this module, kept at the edge.
pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn ino_of(md: &std::fs::Metadata) -> u64 {
    use std::os::unix::fs::MetadataExt;
    md.ino()
}

/// Without inodes the size is the only identity available, so a same-size
/// rebuild is not detected. Stated rather than hidden: the verdict then errs
/// toward "current", which is the direction this whole module exists to fix.
#[cfg(not(unix))]
fn ino_of(_md: &std::fs::Metadata) -> u64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(ino: u64, size: u64) -> BuildSnapshot {
        BuildSnapshot {
            pid: 73527,
            binary: PathBuf::from("/tmp/toron"),
            started_at_unix: 1_790_766_773,
            built_at_unix: 1_790_766_298,
            ino,
            size,
        }
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "toron-serve-build-{tag}-{}-{}",
            std::process::id(),
            now_unix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    /// The whole point: a daemon still executing the file it booted from is not
    /// a problem, and saying otherwise would train the reader to ignore it.
    #[test]
    fn verdict_is_current_when_the_served_artifact_is_unchanged() {
        let s = snap(42, 1_048_576);
        assert_eq!(
            build_verdict(&s, Some(73527), Some(s.artifact())),
            BuildVerdict::Current
        );
        assert!(!build_verdict(&s, Some(73527), Some(s.artifact())).is_replaced());
    }

    /// A rebuild renames a new file over the old one, so the inode moves while
    /// the size can stay put. This is the case that cost a debug cycle: the
    /// daemon keeps serving the overwritten file.
    #[test]
    fn verdict_is_replaced_when_the_artifact_was_rebuilt_after_boot() {
        let s = snap(42, 1_048_576);
        let rebuilt = ArtifactId {
            ino: 43,
            size: s.size,
        };
        assert_eq!(
            build_verdict(&s, Some(73527), Some(rebuilt)),
            BuildVerdict::Replaced
        );
        assert!(build_verdict(&s, Some(73527), Some(rebuilt)).is_replaced());
        // Same inode, different length is a rebuild too (in-place writer).
        let resized = ArtifactId {
            ino: s.ino,
            size: s.size + 16,
        };
        assert_eq!(
            build_verdict(&s, Some(73527), Some(resized)),
            BuildVerdict::Replaced
        );
    }

    /// `cargo clean`, or a switch to a release path, leaves the booted artifact
    /// gone. That is not "fine" and must not read as Current.
    #[test]
    fn verdict_is_replaced_when_the_artifact_is_gone() {
        let s = snap(42, 1_048_576);
        assert_eq!(build_verdict(&s, Some(73527), None), BuildVerdict::Replaced);
    }

    /// Another process owns the address: the record is from an earlier boot, so
    /// judging it would name the wrong binary. Refuse, do not guess.
    #[test]
    fn verdict_refuses_to_judge_a_record_from_another_process() {
        let s = snap(42, 1_048_576);
        assert_eq!(
            build_verdict(&s, Some(999), Some(s.artifact())),
            BuildVerdict::NotServing
        );
        assert_eq!(
            build_verdict(&s, None, Some(s.artifact())),
            BuildVerdict::NotServing
        );
    }

    /// The identity must actually move when a file is rewritten — otherwise the
    /// verdict above would be checking a constant.
    #[test]
    fn artifact_identity_tracks_a_rewrite() {
        let dir = scratch("id");
        let path = dir.join("toron");
        std::fs::write(&path, b"first build").expect("write");
        let before = artifact_id(&path).expect("stat");
        assert_eq!(before.size, 11);

        // Same path, different artifact — as a rebuild leaves it.
        std::fs::write(&path, b"second build, longer").expect("rewrite");
        let after = artifact_id(&path).expect("restat");
        assert_ne!(before, after, "a rewrite must change the identity");
        assert_eq!(after.size, 20);

        // And a missing file has no identity to compare (never a false Current).
        assert_eq!(artifact_id(&dir.join("absent")), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The record must survive a round trip, and a malformed one must read as
    /// no record at all rather than as a verdict.
    #[test]
    fn record_round_trips_and_malformed_reads_as_absent() {
        let dir = scratch("record");
        let path = dir.join(SERVE_BUILD_FILE);
        let s = snap(42, 1_048_576);
        record(&path, &s).expect("record");
        assert_eq!(read(&path), Some(s.clone()));

        std::fs::write(&path, b"{ truncated").expect("corrupt");
        assert_eq!(read(&path), None, "a half-written record is not a verdict");

        remove(&path);
        assert_eq!(read(&path), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A snapshot of a real file carries that file's identity and build time,
    /// which is what makes the record trustworthy in the first place.
    #[test]
    fn snapshot_of_reads_the_artifact_it_names() {
        let dir = scratch("snap");
        let path = dir.join("toron");
        std::fs::write(&path, b"artifact").expect("write");

        let s = snapshot_of(&path, 1234, 1_790_766_773).expect("snapshot");
        assert_eq!(s.pid, 1234);
        assert_eq!(s.started_at_unix, 1_790_766_773);
        assert_eq!(s.size, 8);
        assert!(
            s.built_at_unix > 0,
            "build time comes from the file's mtime"
        );
        assert_eq!(artifact_mtime_unix(&path), Some(s.built_at_unix));
        assert_eq!(artifact_mtime_unix(&dir.join("absent")), None);
        assert_eq!(artifact_id(&path), Some(s.artifact()));
        assert_eq!(
            build_verdict(&s, Some(1234), artifact_id(&path)),
            BuildVerdict::Current
        );
        assert_eq!(snapshot_of(&dir.join("absent"), 1, 0), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
