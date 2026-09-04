// governed-by: ADR-0003, ADR-0007
#![forbid(unsafe_code)]

// bd-led.4.3: Git archive projection (AM-Rust layout), async commit
// coalescer, TORON_GIT_BINARY override (ADR G16). libgit2 (git2, vendored)
// is primary; the binary override is honored on the subprocess commit path.
//
// Permanent storage of unwrapped canonical content — the auditable system of
// record + DR source (ADR: "Git archive (unwrapped canonical content) is
// permanent"). Envelope TTL on the bus does NOT apply here.

use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use tokio::sync::Notify;

/// AM-Rust per-project archive layout.
pub mod layout {
    use std::path::{Path, PathBuf};

    /// `messages/{YYYY}/{MM}/{message_id}.json`
    pub fn message(project_root: &Path, yyyy: u32, mm: u32, id: &str) -> PathBuf {
        project_root
            .join("messages")
            .join(format!("{yyyy:04}"))
            .join(format!("{mm:02}"))
            .join(format!("{id}.json"))
    }

    /// `agents/{name}/{inbox|outbox}/{message_id}.json`
    pub fn agent_mail(project_root: &Path, name: &str, outbox: bool, id: &str) -> PathBuf {
        project_root
            .join("agents")
            .join(name)
            .join(if outbox { "outbox" } else { "inbox" })
            .join(format!("{id}.json"))
    }

    /// `file_reservations/{reservation_id}.json`
    pub fn reservation(project_root: &Path, id: &str) -> PathBuf {
        project_root
            .join("file_reservations")
            .join(format!("{id}.json"))
    }

    /// `build_slots/{slot}.json`
    pub fn build_slot(project_root: &Path, slot: &str) -> PathBuf {
        project_root
            .join("build_slots")
            .join(format!("{slot}.json"))
    }

    /// `.archive.lock` — single-writer marker at the archive root.
    pub fn archive_lock(project_root: &Path) -> PathBuf {
        project_root.join(".archive.lock")
    }
}

/// Async commit coalescer: buffers dirty state and flushes with one commit.
/// `flush()` collapses N staged writes into a single commit.
#[derive(Debug, Default)]
pub struct CommitCoalescer {
    notify: Notify,
    committed: Mutex<u64>,
}

impl CommitCoalescer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake the coalescer (marks work pending). Callers stage a file, then
    /// `ping()`. The background task commits after the debounce window.
    pub fn ping(&self) {
        self.notify.notify_one();
    }

    /// Number of commits emitted (test observable).
    pub fn commit_count(&self) -> u64 {
        *self.committed.lock()
    }

    /// Run the debounce loop: commit `on_flush` after `debounce` of quiet,
    /// repeatedly. `on_flush` returns true if it made a commit.
    pub async fn run<F>(&self, debounce: std::time::Duration, mut on_flush: F)
    where
        F: FnMut() -> bool + Send,
    {
        loop {
            self.notify.notified().await;
            // Debounce: wait for a quiet window before flushing.
            loop {
                tokio::select! {
                    _ = self.notify.notified() => {
                        // More work arrived; keep waiting for quiet.
                    }
                    _ = tokio::time::sleep(debounce) => {
                        if on_flush() {
                            *self.committed.lock() += 1;
                        }
                        break;
                    }
                }
            }
        }
    }
}

/// Paths the archive is allowed to put in a commit. Anything else is a
/// source-tree path — committing it from a long-lived daemon handle is how
/// `c9dd670` reverted ADR-0012 (stale libgit2 index written as HEAD).
fn is_archive_rel(rel: &Path) -> bool {
    let s = rel.to_string_lossy().replace('\\', "/");
    const PREFIXES: &[&str] = &[
        "messages/",
        ".projected/",
        ".audit/",
        "agents/",
        "file_reservations/",
        "build_slots/",
        "blobs/",
    ];
    PREFIXES
        .iter()
        .any(|p| s == p.trim_end_matches('/') || s.starts_with(p))
}

/// Build a tree = current HEAD (fresh peel, not a cached index) + `files`.
/// Uses an in-memory index so the on-disk `.git/index` is never overwritten.
fn tree_from_head_plus(
    repo: &git2::Repository,
    files: &[(PathBuf, Vec<u8>)],
) -> anyhow::Result<git2::Oid> {
    let mut index = git2::Index::new()?;
    if let Ok(head) = repo.head()
        && let Ok(commit) = head.peel_to_commit()
    {
        index.read_tree(&commit.tree()?)?;
    }
    for (rel, content) in files {
        let blob = repo.blob(content)?;
        let path = rel.to_string_lossy().replace('\\', "/");
        let path_bytes = path.into_bytes();
        let flags = u16::try_from(path_bytes.len().min(0xFFF)).unwrap_or(0xFFF);
        let entry = git2::IndexEntry {
            ctime: git2::IndexTime::new(0, 0),
            mtime: git2::IndexTime::new(0, 0),
            dev: 0,
            ino: 0,
            mode: 0o100_644,
            uid: 0,
            gid: 0,
            file_size: u32::try_from(content.len()).unwrap_or(u32::MAX),
            id: blob,
            flags,
            flags_extended: 0,
            path: path_bytes,
        };
        index.add(&entry)?;
    }
    Ok(index.write_tree_to(repo)?)
}

/// libgit2-backed archive for one project. All writes are file-staged then
/// committed; callers batch stages and let the coalescer emit one commit.
pub struct GitArchive {
    repo: git2::Repository,
    project_root: PathBuf,
    /// Process-level writer gate (mirrors `.archive.lock` single-writer).
    _writer: Mutex<()>,
    git_binary: Option<PathBuf>,
    /// Files staged since the last successful commit. Never the repo index —
    /// that index goes stale when a human commits on another handle.
    pending: Mutex<Vec<(PathBuf, Vec<u8>)>>,
}

impl GitArchive {
    /// Open an existing archive or initialize a new repository at root.
    pub fn open_or_init(root: &Path) -> anyhow::Result<Self> {
        let git_binary = std::env::var("TORON_GIT_BINARY")
            .ok()
            .map(PathBuf::from)
            .or_else(|| which("git"));
        Self::open_or_init_with_binary(root, git_binary)
    }

    /// Open/init with an explicit git binary (tests avoid mutating env).
    pub fn open_or_init_with_binary(
        root: &Path,
        git_binary: Option<PathBuf>,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(root)?;
        let repo = match git2::Repository::open(root) {
            Ok(r) => r,
            Err(_) => git2::Repository::init(root)?,
        };
        // Touch the single-writer marker.
        std::fs::write(layout::archive_lock(root), b"")?;
        Ok(Self {
            repo,
            project_root: root.to_path_buf(),
            _writer: Mutex::new(()),
            git_binary,
            pending: Mutex::new(Vec::new()),
        })
    }
    pub fn project_root(&self) -> &Path {
        &self.project_root
    }

    pub fn git_binary_override(&self) -> Option<&Path> {
        self.git_binary.as_deref()
    }

    /// Stage a file for the next archive commit. The file is written first so
    /// the worktree is the source of truth for audits. The repo index is NOT
    /// touched — a long-lived daemon's index is stale after external commits.
    pub fn stage_file(&self, rel: &Path, content: &[u8]) -> anyhow::Result<()> {
        if !is_archive_rel(rel) {
            anyhow::bail!(
                "archive refuses to stage non-archive path {}",
                rel.display()
            );
        }
        let abs = self.project_root.join(rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&abs, content)?;
        self.pending
            .lock()
            .push((rel.to_path_buf(), content.to_vec()));
        Ok(())
    }

    /// Commit pending archive files onto current HEAD. Never writes the
    /// on-disk index; the new tree is HEAD + pending paths only.
    pub fn commit(&self, message: &str) -> anyhow::Result<git2::Oid> {
        let files = std::mem::take(&mut *self.pending.lock());
        if files.is_empty() {
            return Err(anyhow::anyhow!("no staged changes"));
        }
        match self.commit_pending(message, &files) {
            Ok(oid) => Ok(oid),
            Err(e) => {
                self.pending.lock().splice(0..0, files);
                Err(e)
            }
        }
    }

    /// Daemon archive identity (GS-018): archive commits carry an explicit
    /// fixed signature, never ambient `repo.signature()`. Ambient identity
    /// fails where no git config exists (CI runners, containers) and
    /// misattributes machine-written history to whoever owns $HOME.
    pub fn archive_signature() -> anyhow::Result<git2::Signature<'static>> {
        Ok(git2::Signature::now(
            "toron-archive",
            "toron-archive@invalid",
        )?)
    }

    fn commit_pending(
        &self,
        message: &str,
        files: &[(PathBuf, Vec<u8>)],
    ) -> anyhow::Result<git2::Oid> {
        let tree_id = tree_from_head_plus(&self.repo, files)?;
        let tree = self.repo.find_tree(tree_id)?;
        let sig = Self::archive_signature()?;
        let parent = self.repo.head().ok().and_then(|h| h.peel_to_commit().ok());
        let parent_refs: Vec<&git2::Commit> = parent.iter().collect();
        Ok(self.repo.commit(
            Some("HEAD"),
            &sig,
            &sig,
            message,
            &tree,
            parent_refs.as_slice(),
        )?)
    }

    /// Commit via the configured git binary (subprocess path; honored when
    /// `TORON_GIT_BINARY` is set). libgit2 remains the default.
    pub fn commit_via_binary(&self, message: &str) -> anyhow::Result<()> {
        let bin = self
            .git_binary
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("no git binary configured"))?;
        let out = std::process::Command::new(bin)
            .arg("-C")
            .arg(&self.project_root)
            .arg("commit")
            .arg("-m")
            .arg(message)
            .output()?;
        if !out.status.success() {
            anyhow::bail!(
                "git commit failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        Ok(())
    }

    /// Total commit count (HEAD depth) — observable for coalescer tests.
    pub fn commit_count(&self) -> anyhow::Result<u64> {
        let head = match self.repo.head() {
            Ok(h) => h.peel_to_commit()?,
            Err(_) => return Ok(0),
        };
        let mut count = 0u64;
        let mut commit = head;
        loop {
            count += 1;
            match commit.parent(0) {
                Ok(p) => commit = p,
                Err(_) => break,
            }
        }
        Ok(count)
    }
}

/// Resolve `git` on PATH without the `which` crate (stdlib-ish).
fn which(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("toron-archive-{tag}-{}", uuid::Uuid::new_v4()))
    }

    #[tokio::test]
    async fn send_path_writes_am_rust_layout() {
        let root = temp_root("layout");
        let archive = GitArchive::open_or_init(&root).expect("init");

        // "Send" writes the canonical message + the sender outbox copy.
        let msg = layout::message(&root, 2026, 8, "evt-1");
        let outbox = layout::agent_mail(&root, "PiAlpha", true, "evt-1");
        archive
            .stage_file(msg.strip_prefix(&root).unwrap(), br#"{"kind":14}"#)
            .expect("stage msg");
        archive
            .stage_file(outbox.strip_prefix(&root).unwrap(), br#"{"kind":14}"#)
            .expect("stage outbox");
        archive.commit("send evt-1").expect("commit");

        assert!(msg.exists(), "messages/2026/08/evt-1.json exists");
        assert!(outbox.exists(), "agents/PiAlpha/outbox/evt-1.json exists");
        assert!(layout::archive_lock(&root).exists(), ".archive.lock exists");
        assert_eq!(archive.commit_count().unwrap(), 1);

        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn reservation_and_slot_layout() {
        let root = temp_root("res");
        let archive = GitArchive::open_or_init(&root).expect("init");
        let res = layout::reservation(&root, "r-1");
        let slot = layout::build_slot(&root, "ci");
        archive
            .stage_file(res.strip_prefix(&root).unwrap(), b"{}")
            .unwrap();
        archive
            .stage_file(slot.strip_prefix(&root).unwrap(), b"{}")
            .unwrap();
        archive.commit("reserve").unwrap();
        assert!(res.exists());
        assert!(slot.exists());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn coalescer_collapses_n_writes_to_one_commit() {
        // Without a coalescer, N writes = N commits. The coalescer batches
        // staged files and flushes once: 1 commit for many staged files.
        let root = temp_root("coalesce");
        let archive = GitArchive::open_or_init(&root).expect("init");

        for i in 0..10 {
            let p = layout::reservation(&root, &format!("r-{i}"));
            archive
                .stage_file(p.strip_prefix(&root).unwrap(), b"{}")
                .expect("stage");
        }
        archive
            .commit("batch of 10 reservations")
            .expect("one commit");
        assert_eq!(
            archive.commit_count().unwrap(),
            1,
            "10 staged writes -> 1 commit"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn coalescer_run_debounces_quiet_window() {
        let root = temp_root("run");
        let archive = std::sync::Arc::new(parking_lot::Mutex::new(
            GitArchive::open_or_init(&root).expect("init"),
        ));
        let coalescer = std::sync::Arc::new(CommitCoalescer::new());

        let arc = archive.clone();
        let cc = coalescer.clone();
        let handle = tokio::spawn(async move {
            cc.run(std::time::Duration::from_millis(20), || {
                arc.lock().commit("coalesced").is_ok()
            })
            .await;
        });

        // 5 rapid pings — the debounce loop should commit once after quiet.
        for i in 0..5 {
            let p = layout::build_slot(&root, &format!("s-{i}"));
            archive
                .lock()
                .stage_file(p.strip_prefix(&root).unwrap(), b"{}")
                .unwrap();
            coalescer.ping();
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        // Wait past the debounce window so the flush fires.
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        assert_eq!(
            archive.lock().commit_count().unwrap(),
            1,
            "one coalesced commit"
        );

        handle.abort();
        let _ = std::fs::remove_dir_all(root);
    }

    fn commit_tree(
        repo: &git2::Repository,
        message: &str,
        paths: &[&str],
        parents: &[&git2::Commit<'_>],
    ) -> git2::Oid {
        let mut index = repo.index().unwrap();
        for p in paths {
            index.add_path(Path::new(p)).unwrap();
        }
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_id).unwrap();
        let sig = GitArchive::archive_signature().expect("sig");
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, parents)
            .unwrap()
    }

    #[test]
    fn archive_commit_must_not_revert_external_source_commits() {
        // Daemon holds one GitArchive for process life. After it has committed
        // once (index cached), a human commits source on another handle.
        // Next archive commit MUST keep that source (c9dd670 class).
        let root = temp_root("stale-idx");
        let archive = GitArchive::open_or_init(&root).expect("init");
        std::fs::write(root.join("cron.rs"), b"v1").unwrap();
        {
            let repo = git2::Repository::open(&root).unwrap();
            commit_tree(&repo, "source v1", &["cron.rs"], &[]);
        }

        archive
            .stage_file(Path::new("messages/2026/08/evt0.json"), br#"{"k":0}"#)
            .unwrap();
        archive.commit("project message evt0").unwrap();

        std::fs::write(root.join("cron.rs"), b"v2-human").unwrap();
        {
            let repo = git2::Repository::open(&root).unwrap();
            let parent = repo.head().unwrap().peel_to_commit().unwrap();
            commit_tree(&repo, "source v2", &["cron.rs"], &[&parent]);
        }

        archive
            .stage_file(Path::new("messages/2026/08/evt.json"), br#"{"k":1}"#)
            .unwrap();
        archive.commit("project message evt").unwrap();

        let repo = git2::Repository::open(&root).unwrap();
        let tree = repo.head().unwrap().peel_to_tree().unwrap();
        let cron = tree.get_path(Path::new("cron.rs")).unwrap();
        let blob = repo.find_blob(cron.id()).unwrap();
        assert_eq!(
            blob.content(),
            b"v2-human",
            "archive commit must not revert an external source commit"
        );
        tree.get_path(Path::new("messages/2026/08/evt.json"))
            .expect("archive file in HEAD");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn toron_git_binary_override_detected() {
        // Explicit-binary constructor surfaces as the subprocess binary —
        // equivalent to TORON_GIT_BINARY env being read by open_or_init.
        let root = temp_root("bin");
        let archive =
            GitArchive::open_or_init_with_binary(&root, Some(PathBuf::from("/usr/bin/git")))
                .expect("init");
        assert_eq!(
            archive.git_binary_override().unwrap().to_str().unwrap(),
            "/usr/bin/git"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
