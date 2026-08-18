// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Pre-commit guard (ADR-0003 G10 + AM-Rust parity): block commits that
//! stage paths exclusively reserved by ANOTHER agent; a commit touching only
//! your own (or unreserved) paths is never blocked.
//!
//! Semantics mirror AM-Rust's symmetric fnmatch reservation conflict:
//! a staged file conflicts when another agent's active exclusive reservation
//! pattern matches it. `TORON_GIT_BINARY` (libgit2 override, G16) is honored
//! on the subprocess path when a binary override is configured.
//!
//! The guard is installed as a `.git/hooks/pre-commit` script. The script
//! shells out to the `toron` CLI (`guard check` subcommand); the CLI subcommand
//! (toron-cli, oc-c lane) calls back into `guard_check_paths` below.

use std::path::Path;

/// An active file reservation as seen by the guard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// Holder's agent name (project-scoped; name → npub via kind-30013).
    pub holder: String,
    /// The reserved path glob (e.g. `src/**`). AM-Rust fnmatch semantics.
    pub pattern: String,
    /// Exclusive reservations block others; shared reservations do not.
    pub exclusive: bool,
}

/// Check whether a commit's staged paths violate any OTHER agent's exclusive
/// reservation. Returns the offending paths (empty = commit allowed).
///
/// - Own reservations never block (holder == agent).
/// - A path is blocked iff some other agent holds an active exclusive
///   reservation whose pattern matches it (symmetric fnmatch).
///
/// `agent` is `Option` for the CLI path: an unattributed commit (no agent
/// resolvable from `--as` / `TORON_AGENT` / `FLYWHEEL_MAIL_AS`) cannot be
/// told apart from the holder, so it FAILS OPEN (empty result) — the guard
/// only blocks when attribution is available. This keeps human/operator
/// commits (which carry no agent env) unblocked.
pub fn guard_check_blocked(
    agent: Option<&str>,
    staged: &[String],
    reservations: &[Reservation],
) -> Vec<String> {
    match agent {
        None => Vec::new(),
        Some(agent) => guard_blocked_paths(agent, staged, reservations),
    }
}

/// Check whether a commit's staged paths violate any OTHER agent's exclusive
/// reservation. Returns the offending paths (empty = commit allowed).
///
/// - Own reservations never block (holder == agent).
/// - A path is blocked iff some other agent holds an active exclusive
///   reservation whose pattern matches it (symmetric fnmatch).
///
/// bd-pin-public-plane-s-easy.1: holder equality goes through the resolver —
/// pin and slug are the same holder when `resolve` maps them to one
/// canonical name (a fold slug → pin). Without a resolver, names compare
/// raw (identity) — legacy behavior preserved for callers that cannot
/// resolve.
pub fn guard_blocked_paths(
    agent: &str,
    staged: &[String],
    reservations: &[Reservation],
) -> Vec<String> {
    guard_blocked_paths_resolved(agent, staged, reservations, &|n| n.to_string())
}

/// bd-pin-public-plane-s-easy.1: `guard_blocked_paths` with a name resolver.
/// Holder equality is `resolve(holder) == resolve(agent)` — never a raw
/// string compare across aliases (a reservation held by the pin blocks a
/// slug-attributed commit of the SAME agent as itself, and vice versa).
pub fn guard_blocked_paths_resolved(
    agent: &str,
    staged: &[String],
    reservations: &[Reservation],
    resolve: &dyn Fn(&str) -> String,
) -> Vec<String> {
    staged
        .iter()
        .filter(|path| {
            reservations.iter().any(|r| {
                r.exclusive
                    && resolve(&r.holder) != resolve(agent)
                    && pattern_matches(&r.pattern, path)
            })
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// bd-2dox strict sweep-guard: every staged path must be covered by the
// committer's OWN active reservation or the shared-safe bookkeeping list.
// ---------------------------------------------------------------------------

/// Shared-safe bookkeeping paths: tracker files every lane must touch to
/// claim/close beads — never require a reservation of their own.
pub const SHARED_SAFE_PATHS: &[&str] = &[".beads/issues.jsonl", ".beads/beads.base.jsonl"];

/// Why a staged path fails the strict guard (bd-2dox).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictBlockKind {
    /// No agent attribution — the commit cannot be verified (fail-closed).
    Unattributed,
    /// Another agent holds an active exclusive reservation on the path.
    HeldBy(String),
    /// Staged but covered by no active reservation at all.
    Unreserved,
}

/// One strict-guard violation: the offending path + why it blocked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StrictBlock {
    pub path: String,
    pub kind: StrictBlockKind,
}

/// Strict sweep-guard (bd-2dox): every staged path must match (a) the
/// committing agent's OWN active reservation (exclusive or shared — a
/// shared lease is still the agent's own coverage, it never blocks others),
/// or (b) the shared-safe bookkeeping list. Violations return per path with
/// the reason; empty = commit allowed.
///
/// FAIL-CLOSED on attribution: `agent == None` blocks every staged path
/// with `Unattributed` — an unattributed commit cannot be verified and must
/// never silently pass (root cause 2: the legacy `guard_check_blocked`
/// fails open). The legacy function stays byte-identical for non-strict
/// surfaces.
pub fn guard_check_strict(
    agent: Option<&str>,
    staged: &[String],
    reservations: &[Reservation],
    shared_safe: &[&str],
) -> Vec<StrictBlock> {
    guard_check_strict_resolved(agent, staged, reservations, shared_safe, &|n| n.to_string())
}

/// bd-pin-public-plane-s-easy.1: `guard_check_strict` with a name resolver —
/// pin and slug are the same holder when `resolve` maps them to one
/// canonical name (a fold slug → pin). Own-coverage and held-by comparison
/// go through `resolve`, never a raw string compare across aliases.
pub fn guard_check_strict_resolved(
    agent: Option<&str>,
    staged: &[String],
    reservations: &[Reservation],
    shared_safe: &[&str],
    resolve: &dyn Fn(&str) -> String,
) -> Vec<StrictBlock> {
    let Some(agent) = agent else {
        return staged
            .iter()
            .map(|p| StrictBlock {
                path: p.clone(),
                kind: StrictBlockKind::Unattributed,
            })
            .collect();
    };
    let mut blocked = Vec::new();
    for path in staged {
        if shared_safe.contains(&path.as_str()) {
            continue;
        }
        let own = reservations
            .iter()
            .any(|r| resolve(&r.holder) == resolve(agent) && pattern_matches(&r.pattern, path));
        if own {
            continue;
        }
        match reservations.iter().find(|r| {
            r.exclusive && resolve(&r.holder) != resolve(agent) && pattern_matches(&r.pattern, path)
        }) {
            Some(r) => blocked.push(StrictBlock {
                path: path.clone(),
                kind: StrictBlockKind::HeldBy(r.holder.clone()),
            }),
            None => blocked.push(StrictBlock {
                path: path.clone(),
                kind: StrictBlockKind::Unreserved,
            }),
        }
    }
    blocked
}

/// fnmatch-style glob matcher (AM-Rust parity): supports `*`, `?`, `**`,
/// and `[...]` character classes. `*` matches any run of chars (including
/// `/`); `**` is equivalent to `*` for path matching (fnmatch semantics).
pub fn pattern_matches(pattern: &str, path: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let text: Vec<char> = path.chars().collect();
    match_impl(&pat, &text, 0, 0)
}

fn match_impl(pat: &[char], text: &[char], pi: usize, ti: usize) -> bool {
    if pi == pat.len() {
        return ti == text.len();
    }
    match pat[pi] {
        '*' => {
            // Collapse `**` runs into a single `*` (fnmatch treats them the
            // same for path matching).
            let mut next = pi;
            while next < pat.len() && pat[next] == '*' {
                next += 1;
            }
            if next == pat.len() {
                return true;
            }
            // Try every split point in the remaining text.
            for k in ti..=text.len() {
                if match_impl(pat, text, next, k) {
                    return true;
                }
            }
            false
        }
        '?' => ti < text.len() && match_impl(pat, text, pi + 1, ti + 1),
        '[' => {
            if ti >= text.len() {
                return false;
            }
            match parse_class(pat, pi) {
                Some((neg, end)) => {
                    let contained = class_contains(pat, pi + 1, end - 1, text[ti]);
                    (if neg { !contained } else { contained }) && match_impl(pat, text, end, ti + 1)
                }
                None => text[ti] == '[' && match_impl(pat, text, pi + 1, ti + 1),
            }
        }
        c => ti < text.len() && text[ti] == c && match_impl(pat, text, pi + 1, ti + 1),
    }
}

/// Parse `[abc]`, `[a-z]`, `[!abc]` at `start` (which points at `[`).
/// Returns `(negated, end_index_after_closing_bracket)`.
fn parse_class(pat: &[char], start: usize) -> Option<(bool, usize)> {
    let mut i = start + 1;
    if i >= pat.len() {
        return None;
    }
    let neg = pat[i] == '!' || pat[i] == '^';
    if neg {
        i += 1;
    }
    // Empty class `[]` / `[!]` are literal `[`.
    if i >= pat.len() || pat[i] == ']' {
        return None;
    }
    while i < pat.len() && pat[i] != ']' {
        i += 1;
    }
    if i >= pat.len() {
        None
    } else {
        Some((neg, i + 1))
    }
}

/// Does the class between `lo` (exclusive of `[`/`!`) and `hi` (inclusive of
/// the last class char, exclusive of `]`) contain `c`? Supports `a-z` ranges.
fn class_contains(pat: &[char], lo: usize, hi: usize, c: char) -> bool {
    let mut i = lo;
    while i <= hi {
        if i + 2 <= hi && pat[i + 1] == '-' {
            let (a, b) = (pat[i], pat[i + 2]);
            if (a..=b).contains(&c) {
                return true;
            }
            i += 3;
        } else {
            if pat[i] == c {
                return true;
            }
            i += 1;
        }
    }
    false
}

/// Install the toron pre-commit guard into a git repository's hooks directory.
///
/// Uses libgit2 to locate `.git/hooks/pre-commit`. The script delegates to the
/// `toron` CLI's `guard check` subcommand (wired in toron-cli); the project
/// slug is baked into the invocation so the guard evaluates the right
/// project's reservations. `TORON_GIT_BINARY` is exported for the subprocess
/// so the archive commit path honors the override.
pub fn install_precommit_guard(repo_path: &Path, project: &str) -> anyhow::Result<PathBuf> {
    use std::io::Write;

    let repo = git2::Repository::open(repo_path)?;
    let git_dir = repo.path();
    let hooks = git_dir.join("hooks");
    std::fs::create_dir_all(&hooks)?;
    let script_path = hooks.join("pre-commit");
    let legacy_path = hooks.join("pre-commit.legacy");

    // bd-2dox: never clobber a pre-existing hook (the legacy mcp-agent-mail
    // chain-runner in the flywheel repo). Preserve it as pre-commit.legacy
    // and chain it AFTER the guard. NOT pre-commit.orig: the legacy
    // chain-runner itself looks for pre-commit.orig and would exec itself
    // (infinite recursion) if parked there. If BOTH a foreign pre-commit
    // and a pre-commit.legacy exist, refuse — there is no lossless place
    // for the legacy hook.
    let existing_is_toron = script_path.exists()
        && std::fs::read_to_string(&script_path)
            .map(|s| s.contains("installed by toron"))
            .unwrap_or(false);
    if script_path.exists() && !existing_is_toron {
        if legacy_path.exists() {
            anyhow::bail!(
                "cannot install toron guard: {legacy_path:?} already exists (would clobber a \
                 preserved hook). Move it aside and retry."
            );
        }
        std::fs::rename(&script_path, &legacy_path)?;
    }

    let git_binary_override = std::env::var("TORON_GIT_BINARY").unwrap_or_default();
    let export = if git_binary_override.is_empty() {
        String::new()
    } else {
        format!("export TORON_GIT_BINARY={git_binary_override}\n")
    };

    let script = format!(
        "#!/bin/sh\n\
         # installed by toron — governed-by: ADR-0003\n\
         # Strict staged-paths sweep-guard (bd-2dox): a commit may only stage\n\
         # paths covered by the committer's OWN active reservation (or the\n\
         # .beads bookkeeping files). Attribution: set AGENT_NAME=<pin>\n\
         # (or TORON_AGENT / FLYWHEEL_MAIL_AS).\n\
         # Session escape hatch: TORON_GUARD_STRICT=0 <commit> (logged).\n\
         # One mailbox (bd-pin-public-plane-s-easy.14): guard check reads the\n\
         # reservation roster from the toron daemon when TORON_MCP_URL is set\n\
         # (or 127.0.0.1:18080 serves); otherwise it embeds against the durable\n\
         # XDG root. Never a second mailbox.\n\
         {export}\
         if command -v toron >/dev/null 2>&1; then\n\
         \texport TORON_GUARD_STRICT=\"${{TORON_GUARD_STRICT:-1}}\"\n\
         \ttoron guard check --repo \"$PWD\" --project {project}\n\
         \trc=$?\n\
         \tif [ \"$rc\" -ne 0 ]; then exit \"$rc\"; fi\n\
         fi\n\
         # chain a pre-existing hook preserved at install (never clobbered)\n\
         if [ -x \"$(dirname \"$0\")/pre-commit.legacy\" ]; then\n\
         \texec \"$(dirname \"$0\")/pre-commit.legacy\" \"$@\"\n\
         fi\n\
         exit 0\n"
    );

    let mut file = std::fs::File::create(&script_path)?;
    file.write_all(script.as_bytes())?;
    set_executable(&script_path)?;
    Ok(script_path)
}

/// Remove the toron-installed pre-commit guard (idempotent).
///
/// Only removes a script that carries the toron marker; a user's own
/// pre-commit hook is never clobbered. When install preserved a legacy hook
/// as `pre-commit.legacy`, uninstall restores it to `pre-commit`.
pub fn uninstall_precommit_guard(repo_path: &Path) -> anyhow::Result<bool> {
    let repo = git2::Repository::open(repo_path)?;
    let script_path = repo.path().join("hooks").join("pre-commit");
    if !script_path.exists() {
        return Ok(false);
    }
    let contents = std::fs::read_to_string(&script_path)?;
    if !contents.contains("installed by toron") {
        return Ok(false);
    }
    std::fs::remove_file(&script_path)?;
    let legacy_path = repo.path().join("hooks").join("pre-commit.legacy");
    if legacy_path.exists() {
        std::fs::rename(&legacy_path, &script_path)?;
    }
    Ok(true)
}

/// Resolve `TORON_GIT_BINARY` to the git2 override path (G16). Returns `None`
/// when the env var is unset (libgit2 is the default engine).
pub fn git_binary_override() -> Option<std::path::PathBuf> {
    std::env::var_os("TORON_GIT_BINARY").map(Into::into)
}

/// Best-effort chmod +x (POSIX). No-op on non-POSIX.
#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

/// The staged-file set for a repo: paths the guard evaluates. Reads the git
/// index (libgit2) and returns unique staged paths relative to the repo root.
///
/// Only paths whose index entry differs from HEAD (or is newly added with no
/// HEAD yet) count as staged — a clean tracked file must never block a commit
/// that merely stages an unrelated path (bd-f5tt live-gate finding).
pub fn staged_paths(repo: &git2::Repository) -> Vec<String> {
    let Ok(index) = repo.index() else {
        return Vec::new();
    };
    let head_tree = repo.head().ok().and_then(|h| h.peel_to_tree().ok());
    let mut out = Vec::new();
    let diff = repo
        .diff_tree_to_index(head_tree.as_ref(), Some(&index), None)
        .ok();
    let Some(diff) = diff else {
        return out;
    };
    for delta in diff.deltas() {
        if let Some(p) = delta.new_file().path().or_else(|| delta.old_file().path())
            && let Some(p) = p.to_str()
        {
            let p = p.to_string();
            if !out.contains(&p) {
                out.push(p);
            }
        }
    }
    out
}

/// Open a repository at `repo_path` and return its staged paths (empty on
/// open/index errors — the guard never fails a commit on unreadable state).
pub fn staged_paths_in(repo_path: &Path) -> Vec<String> {
    match git2::Repository::open(repo_path) {
        Ok(repo) => staged_paths(&repo),
        Err(_) => Vec::new(),
    }
}

use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn res(holder: &str, pattern: &str, exclusive: bool) -> Reservation {
        Reservation {
            holder: holder.into(),
            pattern: pattern.into(),
            exclusive,
        }
    }

    #[test]
    fn test_own_reservation_never_blocks() {
        let staged = vec!["src/lib.rs".to_string(), "src/foo.rs".to_string()];
        let reservations = vec![res("me", "src/**", true)];
        let blocked = guard_blocked_paths("me", &staged, &reservations);
        assert!(blocked.is_empty(), "own reservation must not block");
    }

    #[test]
    fn test_guard_check_blocked_unattributed_fails_open() {
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("other", "src/**", true)];
        let blocked = guard_check_blocked(None, &staged, &reservations);
        assert!(
            blocked.is_empty(),
            "unattributed commit must fail open (no agent to attribute)"
        );
    }

    #[test]
    fn test_guard_check_blocked_attributed_blocks_foreign() {
        let staged = vec!["src/lib.rs".to_string(), "README.md".to_string()];
        let reservations = vec![res("other", "src/**", true)];
        let blocked = guard_check_blocked(Some("me"), &staged, &reservations);
        assert_eq!(blocked, vec!["src/lib.rs".to_string()]);
    }

    #[test]
    fn test_guard_check_blocked_own_reservation_clear() {
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("me", "src/**", true)];
        let blocked = guard_check_blocked(Some("me"), &staged, &reservations);
        assert!(blocked.is_empty(), "own reservation must not block");
    }

    #[test]
    fn test_install_bakes_project_flag() {
        let root = std::env::temp_dir().join(format!(
            "toron-guard-proj-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");

        let hook = install_precommit_guard(&root, "toron").expect("install");
        let contents = std::fs::read_to_string(&hook).expect("read hook");
        assert!(
            contents.contains("guard check --repo \"$PWD\" --project toron"),
            "hook must bake the project slug into the guard check invocation"
        );
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_foreign_exclusive_reservation_blocks() {
        let staged = vec!["src/lib.rs".to_string(), "README.md".to_string()];
        let reservations = vec![res("other", "src/**", true)];
        let blocked = guard_blocked_paths("me", &staged, &reservations);
        assert_eq!(blocked, vec!["src/lib.rs".to_string()]);
    }

    #[test]
    fn test_shared_reservation_does_not_block() {
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("other", "src/**", false)];
        let blocked = guard_blocked_paths("me", &staged, &reservations);
        assert!(blocked.is_empty(), "shared reservation must not block");
    }

    #[test]
    fn test_unreserved_path_never_blocked() {
        let staged = vec!["docs/plan.md".to_string()];
        let reservations = vec![res("other", "src/**", true)];
        let blocked = guard_blocked_paths("me", &staged, &reservations);
        assert!(blocked.is_empty());
    }

    #[test]
    fn test_multiple_foreign_reservations_all_block() {
        let staged = vec![
            "a/x.rs".to_string(),
            "b/y.rs".to_string(),
            "c/z.rs".to_string(),
        ];
        let reservations = vec![res("other", "a/**", true), res("third", "b/**", true)];
        let blocked = guard_blocked_paths("me", &staged, &reservations);
        assert_eq!(blocked, vec!["a/x.rs".to_string(), "b/y.rs".to_string()]);
    }

    #[test]
    fn test_pattern_matches_basic_glob() {
        assert!(pattern_matches("src/**", "src/lib.rs"));
        assert!(pattern_matches("src/**", "src/deep/nested.rs"));
        assert!(!pattern_matches("src/**", "docs/lib.rs"));
        assert!(pattern_matches("*.rs", "main.rs"));
        assert!(!pattern_matches("*.rs", "main.txt"));
        assert!(pattern_matches("src/?ib.rs", "src/lib.rs"));
        assert!(!pattern_matches("src/?ib.rs", "src/glob.rs"));
    }

    #[test]
    fn test_pattern_matches_char_class() {
        assert!(pattern_matches("src/[l]ib.rs", "src/lib.rs"));
        assert!(pattern_matches("src/[a-m]ib.rs", "src/lib.rs"));
        assert!(!pattern_matches("src/[n-z]ib.rs", "src/lib.rs"));
        assert!(pattern_matches("src/[!a-k]ib.rs", "src/lib.rs"));
        assert!(pattern_matches("foo[0-9].rs", "foo5.rs"));
    }

    #[test]
    fn test_install_and_uninstall_guard_roundtrip() {
        let root = std::env::temp_dir().join(format!(
            "toron-guard-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");

        let hook = install_precommit_guard(&root, "demo").expect("install");
        assert!(hook.exists(), "hook script must exist");
        let contents = std::fs::read_to_string(&hook).expect("read hook");
        assert!(contents.contains("installed by toron"));
        assert!(contents.contains("guard check"));

        let removed = uninstall_precommit_guard(&root).expect("uninstall");
        assert!(removed, "toron hook must be removed");
        assert!(!hook.exists(), "hook must be gone");

        let again = uninstall_precommit_guard(&root).expect("uninstall again");
        assert!(!again, "second uninstall is a no-op");
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_uninstall_does_not_clobber_user_hook() {
        let root = std::env::temp_dir().join(format!(
            "toron-guard-user-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let hooks = repo.path().join("hooks");
        std::fs::create_dir_all(&hooks).expect("hooks dir");
        let script_path = hooks.join("pre-commit");
        std::fs::write(&script_path, "#!/bin/sh\necho user hook\n").expect("write user hook");

        let removed = uninstall_precommit_guard(&root).expect("uninstall");
        assert!(!removed, "user hook must NOT be removed");
        assert!(script_path.exists(), "user hook survives");
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_staged_paths_from_index() {
        let root = std::env::temp_dir().join(format!(
            "toron-staged-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("src")).expect("src dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        std::fs::write(root.join("src/lib.rs"), "pub fn x() {}\n").expect("write");
        std::fs::write(root.join("Cargo.toml"), "[package]\n").expect("write");

        let mut index = repo.index().expect("index");
        index.add_path(Path::new("src/lib.rs")).expect("add");
        index.add_path(Path::new("Cargo.toml")).expect("add");
        index.write().expect("write index");

        let staged = staged_paths(&repo);
        assert!(staged.contains(&"src/lib.rs".to_string()));
        assert!(staged.contains(&"Cargo.toml".to_string()));
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_staged_paths_excludes_clean_tracked_files() {
        let root = std::env::temp_dir().join(format!(
            "toron-staged-clean-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("src")).expect("src dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let sig = git2::Signature::now("t", "t@t").expect("sig");
        std::fs::write(root.join("src/lib.rs"), "pub fn x() {}\n").expect("write");
        std::fs::write(root.join("README.md"), "# r\n").expect("write");
        {
            let mut index = repo.index().expect("index");
            index.add_path(Path::new("src/lib.rs")).expect("add");
            index.add_path(Path::new("README.md")).expect("add");
            index.write().expect("write index");
            let tree_id = index.write_tree().expect("tree");
            let tree = repo.find_tree(tree_id).expect("find tree");
            repo.commit(Some("HEAD"), &sig, &sig, "base", &tree, &[])
                .expect("commit");
        }

        // Now stage a NEW file only; lib.rs/README.md stay clean in the index.
        std::fs::create_dir_all(root.join("docs")).expect("docs dir");
        std::fs::write(root.join("docs/plan.md"), "plan\n").expect("write");
        {
            let mut index = repo.index().expect("index");
            index.add_path(Path::new("docs/plan.md")).expect("add");
            index.write().expect("write index");
        }

        let staged = staged_paths(&repo);
        assert_eq!(
            staged,
            vec!["docs/plan.md".to_string()],
            "clean tracked files must NOT appear as staged"
        );
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    // ------------------------------------------------------------------
    // bd-2dox: strict sweep-guard — every staged path must be covered by the
    // committer's OWN active reservation or the shared-safe bookkeeping list.
    // ------------------------------------------------------------------

    #[test]
    fn strict_blocks_cross_agent_exclusive() {
        // B stages A's exclusively-reserved file + B's own file. B's own
        // path passes; A's path is blocked naming the holder.
        let staged = vec!["src/foo.rs".to_string(), "src/b.rs".to_string()];
        let reservations = vec![res("A", "src/foo.rs", true), res("B", "src/b.rs", true)];
        let blocked = guard_check_strict(Some("B"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 1, "only A's path blocks: {blocked:?}");
        assert_eq!(blocked[0].path, "src/foo.rs");
        assert_eq!(blocked[0].kind, StrictBlockKind::HeldBy("A".into()));
    }

    #[test]
    fn strict_allows_own_reserved_paths() {
        let staged = vec!["src/lib.rs".to_string(), "docs/plan.md".to_string()];
        let reservations = vec![res("me", "src/**", true), res("me", "docs/**", true)];
        let blocked = guard_check_strict(Some("me"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert!(
            blocked.is_empty(),
            "own reservations cover every staged path"
        );
    }

    #[test]
    fn strict_own_shared_reservation_covers() {
        // A SHARED lease is still the agent's own active reservation — it
        // covers the staged path in strict mode (it never blocks others).
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("me", "src/**", false)];
        let blocked = guard_check_strict(Some("me"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert!(blocked.is_empty(), "own shared lease covers the path");
    }

    #[test]
    fn strict_allows_beads_bookkeeping_without_reservation() {
        // Tracker bookkeeping files are never reserved; the shared-safe list
        // lets them through so every lane can close beads.
        let staged = vec![
            ".beads/issues.jsonl".to_string(),
            ".beads/beads.base.jsonl".to_string(),
        ];
        let blocked = guard_check_strict(Some("me"), &staged, &[], SHARED_SAFE_PATHS);
        assert!(
            blocked.is_empty(),
            "bookkeeping is shared-safe: {blocked:?}"
        );
    }

    #[test]
    fn strict_blocks_unattributed_fail_closed() {
        // The sweep-guard must NOT fail open: with no agent attribution the
        // committer's staged paths cannot be verified — block with the env
        // message (exit 1, never a silent pass-through).
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("me", "src/**", true)];
        let blocked = guard_check_strict(None, &staged, &reservations, SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].kind, StrictBlockKind::Unattributed);
    }

    #[test]
    fn strict_blocks_unreserved_path() {
        // A staged path covered by NO reservation is a sweep risk — the
        // strict guard requires the committer to reserve what they stage.
        let staged = vec!["src/lib.rs".to_string()];
        let blocked = guard_check_strict(Some("me"), &staged, &[], SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].path, "src/lib.rs");
        assert_eq!(blocked[0].kind, StrictBlockKind::Unreserved);
    }

    #[test]
    fn strict_foreign_shared_lease_never_blocks_own_coverage() {
        // Another agent's SHARED lease does not block; the path is covered
        // by the committer's own exclusive lease.
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("other", "src/**", false), res("me", "src/lib.rs", true)];
        let blocked = guard_check_strict(Some("me"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert!(blocked.is_empty());
    }

    #[test]
    fn strict_mixed_report_names_each_violation() {
        let staged = vec![
            "a/hers.rs".to_string(),           // HeldBy(Other)
            "b/unowned.rs".to_string(),        // Unreserved
            "c/mine.rs".to_string(),           // own — passes
            ".beads/issues.jsonl".to_string(), // shared-safe — passes
        ];
        let reservations = vec![res("Other", "a/**", true), res("me", "c/**", true)];
        let blocked = guard_check_strict(Some("me"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 2, "two violations named: {blocked:?}");
        assert_eq!(
            blocked[0],
            StrictBlock {
                path: "a/hers.rs".into(),
                kind: StrictBlockKind::HeldBy("Other".into())
            }
        );
        assert_eq!(
            blocked[1],
            StrictBlock {
                path: "b/unowned.rs".into(),
                kind: StrictBlockKind::Unreserved
            }
        );
    }

    #[test]
    fn strict_non_strict_paths_unchanged() {
        // Sanity: the legacy fail-open check is untouched (byte-identical
        // behavior for non-strict surfaces).
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("other", "src/**", true)];
        assert!(guard_check_blocked(None, &staged, &reservations).is_empty());
        assert_eq!(
            guard_check_blocked(Some("me"), &staged, &reservations),
            vec!["src/lib.rs".to_string()]
        );
    }

    // --- bd-pin-public-plane-s-easy.1: pin and slug are ONE holder ---

    #[test]
    fn resolved_guard_treats_pin_and_slug_as_same_holder() {
        // A reservation held by the PIN must not block a commit attributed
        // to the SLUG (same holder after the fold slug → pin).
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("BlueRidge", "src/**", true)];
        let resolve = |name: &str| -> String {
            match name {
                "flywheel-toron-coder" => "BlueRidge".to_string(),
                other => other.to_string(),
            }
        };
        let blocked =
            guard_blocked_paths_resolved("flywheel-toron-coder", &staged, &reservations, &resolve);
        assert!(
            blocked.is_empty(),
            "slug commit must not block against pin-held reservation"
        );

        // Reverse: slug-held reservation, pin commit.
        let reservations = vec![res("flywheel-toron-coder", "src/**", true)];
        let blocked = guard_blocked_paths_resolved("BlueRidge", &staged, &reservations, &resolve);
        assert!(blocked.is_empty(), "pin commit vs slug-held reservation");
    }

    #[test]
    fn resolved_strict_guard_treats_pin_and_slug_as_same_holder() {
        let staged = vec!["src/lib.rs".to_string()];
        let reservations = vec![res("BlueRidge", "src/**", true)];
        let resolve = |name: &str| -> String {
            match name {
                "flywheel-toron-coder" => "BlueRidge".to_string(),
                other => other.to_string(),
            }
        };
        let blocked = guard_check_strict_resolved(
            Some("flywheel-toron-coder"),
            &staged,
            &reservations,
            SHARED_SAFE_PATHS,
            &resolve,
        );
        assert!(
            blocked.is_empty(),
            "slug attribution counts as the pin holder's own coverage"
        );

        // A genuinely DIFFERENT holder still blocks (resolved comparison is
        // not a wildcard).
        let reservations = vec![res("OtherPin", "src/**", true)];
        let blocked = guard_check_strict_resolved(
            Some("flywheel-toron-coder"),
            &staged,
            &reservations,
            SHARED_SAFE_PATHS,
            &resolve,
        );
        assert_eq!(blocked.len(), 1, "foreign holder still blocks");
        assert_eq!(blocked[0].kind, StrictBlockKind::HeldBy("OtherPin".into()));
    }

    // --- bd-pin-public-plane-s-easy.10: anti-sweep — strict guard is the control ---

    #[test]
    fn anti_sweep_blocks_staging_path_held_by_another_agent() {
        // Agent A holds src/a.rs (exclusive). Agent B stages src/a.rs →
        // HeldBy(A). The strict guard is the control, not a DM.
        let staged = vec!["src/a.rs".to_string()];
        let reservations = vec![res("A", "src/a.rs", true)];
        let blocked = guard_check_strict(Some("B"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 1, "B staging A's held path must block");
        assert_eq!(blocked[0].path, "src/a.rs");
        assert_eq!(blocked[0].kind, StrictBlockKind::HeldBy("A".into()));
    }

    #[test]
    fn anti_sweep_allows_staging_only_own_reserved_path() {
        // Agent B stages ONLY its own reserved path → passes (no sweep).
        let staged = vec!["src/b.rs".to_string()];
        let reservations = vec![res("A", "src/a.rs", true), res("B", "src/b.rs", true)];
        let blocked = guard_check_strict(Some("B"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert!(
            blocked.is_empty(),
            "B staging only its own reserved path must pass: {blocked:?}"
        );
    }

    #[test]
    fn anti_sweep_unreserved_path_is_unreserved_not_whispered() {
        // A staged path covered by NO reservation is Unreserved (fail-closed
        // sweep control — the anti-sweep contract, not a nudge).
        let staged = vec!["src/other.rs".to_string()];
        let reservations = vec![res("A", "src/a.rs", true)];
        let blocked = guard_check_strict(Some("B"), &staged, &reservations, SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].path, "src/other.rs");
        assert_eq!(blocked[0].kind, StrictBlockKind::Unreserved);
    }

    #[test]
    fn test_install_hook_defaults_strict_on_with_escape_hatch() {
        // bd-pin-public-plane-s-easy.10: the installed hook must default
        // TORON_GUARD_STRICT=1 (strict is the control) and document the
        // operator escape TORON_GUARD_STRICT=0 — never silent fail-open.
        let root = std::env::temp_dir().join(format!(
            "toron-guard-strict-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let hook = install_precommit_guard(&root, "demo").expect("install");
        let contents = std::fs::read_to_string(&hook).expect("read hook");
        assert!(
            contents.contains("TORON_GUARD_STRICT=\"${TORON_GUARD_STRICT:-1}\""),
            "hook must default strict ON (fail-closed), got: {contents}"
        );
        assert!(
            contents.contains("TORON_GUARD_STRICT=0"),
            "hook must document the operator escape TORON_GUARD_STRICT=0"
        );
        assert!(
            contents.contains("guard check"),
            "hook delegates to the guard check"
        );
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_install_preserves_existing_hook_as_legacy_and_chains() {
        // bd-2dox: installing into a repo with a pre-existing non-toron hook
        // (the legacy mcp-agent-mail chain-runner) must preserve it as
        // pre-commit.legacy and chain it AFTER the guard — never clobber.
        // NOT pre-commit.orig: the chain-runner itself execs pre-commit.orig
        // and would recurse infinitely if parked there.
        let root = std::env::temp_dir().join(format!(
            "toron-guard-chain-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let hooks = repo.path().join("hooks");
        std::fs::create_dir_all(&hooks).expect("hooks dir");
        let script_path = hooks.join("pre-commit");
        std::fs::write(
            &script_path,
            "#!/usr/bin/env python3\n# legacy chain-runner\n",
        )
        .expect("write");

        let hook = install_precommit_guard(&root, "demo").expect("install");
        let legacy = hooks.join("pre-commit.legacy");
        assert!(
            legacy.exists(),
            "legacy hook preserved as pre-commit.legacy"
        );
        let contents = std::fs::read_to_string(&hook).expect("read hook");
        assert!(
            contents.contains("TORON_GUARD_STRICT"),
            "strict env default in hook"
        );
        assert!(
            contents.contains("pre-commit.legacy"),
            "hook chains the preserved legacy hook"
        );

        // Uninstall restores the legacy hook.
        let removed = uninstall_precommit_guard(&root).expect("uninstall");
        assert!(removed);
        let restored = std::fs::read_to_string(&script_path).expect("read restored");
        assert!(
            restored.contains("legacy chain-runner"),
            "legacy hook restored"
        );
        assert!(
            !restored.contains("installed by toron"),
            "toron script gone from pre-commit"
        );
        assert!(!legacy.exists(), "legacy consumed on restore");
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn test_install_refuses_to_clobber_when_legacy_occupied() {
        // If BOTH a foreign pre-commit and a pre-commit.legacy exist, install
        // must refuse (cannot preserve without overwriting) — never silent
        // clobber.
        let root = std::env::temp_dir().join(format!(
            "toron-guard-clobber-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let hooks = repo.path().join("hooks");
        std::fs::create_dir_all(&hooks).expect("hooks dir");
        std::fs::write(hooks.join("pre-commit"), "#!/bin/sh\n# foreign\n").expect("write");
        std::fs::write(
            hooks.join("pre-commit.legacy"),
            "#!/bin/sh\n# legacy occupied\n",
        )
        .expect("write");

        let err = install_precommit_guard(&root, "demo").expect_err("must refuse");
        assert!(
            err.to_string().contains("pre-commit.legacy"),
            "error names the occupied path: {err}"
        );
        let still = std::fs::read_to_string(hooks.join("pre-commit")).expect("read");
        assert!(still.contains("foreign"), "foreign hook untouched");
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }
}
