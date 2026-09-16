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
use std::path::PathBuf;

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
    agent.map_or_else(Vec::new, |a| guard_blocked_paths(a, staged, reservations))
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
pub const SHARED_SAFE_PATHS: &[&str] = &[
    ".beads/issues.jsonl",
    ".beads/beads.base.jsonl",
    ".beads/gates.jsonl",
];

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

/// bd-guard-crossproject-lease-er38: translate a sibling-repo lease
/// spelling into a glob relative to the guarded repo. A lease taken on
/// project A as `../<B>/<rest>` names a path under sibling repo B; when
/// the guarded repo IS B, `guard check` evaluates the lease as `<rest>`
/// so ONE reservation satisfies every spelling. Returns `None` when the
/// pattern does not escape into `repo_name` — repo-relative patterns pass
/// through unchanged at the call site, and deep escapes (`../../`) or
/// backslash spellings are never guessed.
pub fn cross_project_pattern(pattern: &str, repo_name: &str) -> Option<String> {
    let rest = pattern.strip_prefix("../")?;
    if rest.is_empty() {
        return None;
    }
    let (first, tail) = match rest.split_once('/') {
        Some((f, t)) => (f, Some(t)),
        None => (rest, None),
    };
    if first != repo_name {
        return None;
    }
    let tail = tail.map(|t| t.trim_start_matches('/'));
    Some(match tail {
        None | Some("") => "**".to_string(),
        Some(t) => t.to_string(),
    })
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

    // bd-9mb8: the bead-hunk attribution check needs the commit MESSAGE,
    // which only exists at the commit-msg stage — pre-commit runs before
    // the editor opens. Install a companion commit-msg hook that passes
    // "$1" through --msg-file. Same courtesy as pre-commit: a foreign
    // hook is never clobbered (ours is recognized by the marker).
    let cm_path = hooks.join("commit-msg");
    let cm_is_toron = cm_path.exists()
        && std::fs::read_to_string(&cm_path)
            .map(|s| s.contains("installed by toron"))
            .unwrap_or(false);
    if !cm_path.exists() || cm_is_toron {
        let cm_script = format!(
            "#!/bin/sh\n\
             # installed by toron — governed-by: ADR-0003\n\
             # Bead-hunk attribution guard (bd-9mb8): bd-* tokens ADDED by\n\
             # this commit's staged diff must appear in the commit message.\n\
             # Foreign bead-tagged code landing unattributed in a peer commit\n\
             # silently breaks bead-to-commit traceability. Escape hatch:\n\
             # TORON_GUARD_STRICT=0 <commit> (logged, same as pre-commit).\n\
             {export}\
             if command -v toron >/dev/null 2>&1; then\n\
             \texport TORON_GUARD_STRICT=\"${{TORON_GUARD_STRICT:-1}}\"\n\
             \ttoron guard check --repo \"$PWD\" --project {project} --msg-file \"$1\"\n\
             \texit $?\n\
             fi\n\
             exit 0\n"
        );
        let mut cm_file = std::fs::File::create(&cm_path)?;
        cm_file.write_all(cm_script.as_bytes())?;
        set_executable(&cm_path)?;
    }

    // bd-jhes GitFact producer: post-commit hook appends ONE JSONL line to the
    // flywheel fact journal on every SUCCESSFUL commit. Best-effort — ANY
    // failure must NEVER block or fail the commit (the hook exits 0 always).
    // Same courtesy as commit-msg: never clobber a foreign hook.
    let pc_path = hooks.join("post-commit");
    let pc_is_toron = pc_path.exists()
        && std::fs::read_to_string(&pc_path)
            .map(|s| s.contains("installed by toron"))
            .unwrap_or(false);
    if !pc_path.exists() || pc_is_toron {
        let pc_script = format!(
            "#!/bin/sh\n\
             # installed by toron — governed-by: ADR-0003\n\
             # GitFact producer (bd-jhes): on a SUCCESSFUL commit appends one\n\
             # JSONL line {{type:\"git_fact\", repo, sha, subject, bead_id, pin}}\n\
             # to the flywheel fact journal (best-effort, NEVER blocks commit).\n\
             # Attribution: AGENT_NAME / TORON_AGENT / FLYWHEEL_MAIL_AS.\n\
             {export}\
             if command -v toron >/dev/null 2>&1; then\n\
             \ttoron guard record --repo \"$PWD\" --project {project} || true\n\
             fi\n\
             exit 0\n"
        );
        let mut pc_file = std::fs::File::create(&pc_path)?;
        pc_file.write_all(pc_script.as_bytes())?;
        set_executable(&pc_path)?;
    }
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
    // bd-9mb8: remove our companion commit-msg hook too — but never a
    // foreign one (same marker rule as install).
    let cm_path = repo.path().join("hooks").join("commit-msg");
    let cm_is_toron = std::fs::read_to_string(&cm_path)
        .map(|s| s.contains("installed by toron"))
        .unwrap_or(false);
    if cm_is_toron {
        std::fs::remove_file(&cm_path)?;
    }
    // bd-jhes: remove our post-commit GitFact producer too — never a foreign hook.
    let pc_path = repo.path().join("hooks").join("post-commit");
    let pc_is_toron = std::fs::read_to_string(&pc_path)
        .map(|s| s.contains("installed by toron"))
        .unwrap_or(false);
    if pc_is_toron {
        let _ = std::fs::remove_file(&pc_path);
    }
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
    git2::Repository::open(repo_path).map_or_else(|_| Vec::new(), |repo| staged_paths(&repo))
}

// ---------------------------------------------------------------------------
// bd-9mb8 bead-hunk attribution: bd-* tokens ADDED by the staged diff must
// appear in the commit message. Foreign bead-tagged code sweeping into a
// peer's shared-tree window otherwise lands with no commit carrying its id —
// breaking commit-before-close traceability while close gates pass on
// technicalities (observed: flywheel a1498f1 absorbed the oxjy fix).
// ---------------------------------------------------------------------------

/// One bead-hunk attribution violation (bd-9mb8): `token` appears on an
/// ADDED line of the staged diff but is absent from the commit message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeadHunkBlock {
    pub token: String,
}

/// Distinct `bd-<id>` tokens in `text`, first-seen order. Token body matches
/// tracker ids: `[a-z0-9-]+` after a literal `bd-`, case-sensitive, with a
/// boundary before the prefix (not `[a-z0-9-]` — so `abd-x` and the flag-ish
/// `x-bd-y` never match) and trailing hyphens trimmed.
fn bd_tokens(text: &str) -> Vec<String> {
    let b = text.as_bytes();
    let mut out: Vec<String> = Vec::new();
    let mut i = 0;
    while i + 3 <= b.len() {
        let boundary = i == 0 || !(b[i - 1].is_ascii_alphanumeric() || b[i - 1] == b'-');
        if boundary && &b[i..i + 3] == b"bd-" {
            let start = i;
            let mut end = start + 3;
            while end < b.len()
                && (b[end].is_ascii_lowercase() || b[end].is_ascii_digit() || b[end] == b'-')
            {
                end += 1;
            }
            // The regex body `[a-z0-9-]+` requires at least one char after
            // the prefix; trailing hyphens are trimmed for prose safety
            // ("bd-x--" → "bd-x") but never past that first body char.
            if end > start + 3 {
                let mut e = end;
                while e > start + 4 && b[e - 1] == b'-' {
                    e -= 1;
                }
                let tok = text[start..e].to_string();
                if !out.contains(&tok) {
                    out.push(tok);
                }
            }
            i = end;
        } else {
            i += 1;
        }
    }
    out
}

/// Scan ADDED lines of a unified diff for bd-* tokens absent from
/// `message`. Removed/context lines and the `+++`/`---` headers carry no
/// attribution debt; duplicates collapse to one block per token.
pub fn bead_hunk_violations(diff: &str, message: &str) -> Vec<BeadHunkBlock> {
    let msg_tokens = bd_tokens(message);
    let added: String = diff
        .lines()
        .filter(|l| l.starts_with('+') && !l.starts_with("+++"))
        .map(|l| {
            let mut s = l.to_string();
            s.push('\n');
            s
        })
        .collect();
    bd_tokens(&added)
        .into_iter()
        .filter(|t| !msg_tokens.contains(t))
        .map(|token| BeadHunkBlock { token })
        .collect()
}

/// ADDED lines (`+`, excluding `+++` headers) of the staged diff as one
/// text blob, skipping every file whose path is in `exclude`. Empty on any
/// repository/index error — attribution is best-effort input to the check,
/// never a crash surface.
pub fn staged_added_lines_in(repo_path: &Path, exclude: &[&str]) -> String {
    let Ok(repo) = git2::Repository::open(repo_path) else {
        return String::new();
    };
    let Ok(index) = repo.index() else {
        return String::new();
    };
    let head_tree = repo.head().ok().and_then(|h| h.peel_to_tree().ok());
    let Ok(diff) = repo.diff_tree_to_index(head_tree.as_ref(), Some(&index), None) else {
        return String::new();
    };
    let mut current_path = String::new();
    let mut added = String::new();
    let _ = diff.print(git2::DiffFormat::Patch, |delta, _hunk, line| {
        if line.origin() == 'F' {
            current_path.clear();
            if let Some(p) = delta.new_file().path().and_then(|p| p.to_str()) {
                current_path.push_str(p);
            }
        }
        if line.origin() == '+'
            && !exclude.contains(&current_path.as_str())
            && let Ok(c) = std::str::from_utf8(line.content())
        {
            // libgit2 content excludes the origin sign; re-add '+' so the
            // blob is a valid added-lines-only unified diff for
            // [`bead_hunk_violations`]'s line filter.
            added.push('+');
            added.push_str(c);
        }
        true
    });
    added
}

// ---------------------------------------------------------------------------
// bd-jhes GitFact producer: strict guard appends committed facts to the
// flywheel fact journal. Best-effort — ANY failure must NEVER block commit.
// ---------------------------------------------------------------------------

/// Journal path for GitFacts: `$XDG_STATE_HOME/flywheel/journals/git-facts.jsonl`
/// when `XDG_STATE_HOME` is set (hermetic for tests), else the hard-coded
/// fallback `/Users/bangedorrunt/.local/state/flywheel/journals/git-facts.jsonl`
/// named in bd-jhes (which is `$XDG_STATE_HOME` default on this host).
pub fn git_fact_journal_path() -> std::path::PathBuf {
    let xdg = std::env::var_os("XDG_STATE_HOME").filter(|v| !v.is_empty());
    git_fact_journal_path_with_xdg(xdg.as_deref().map(std::path::Path::new))
}

fn git_fact_journal_path_with_xdg(xdg: Option<&Path>) -> std::path::PathBuf {
    if let Some(xdg) = xdg {
        return xdg.join("flywheel/journals/git-facts.jsonl");
    }
    std::path::PathBuf::from("/Users/bangedorrunt/.local/state/flywheel/journals/git-facts.jsonl")
}

/// Extract the first `bd-*` token from a commit message (e.g. `fix: thing
/// [bd-jhes]`). Returns `None` when no token is present. Delegates to the
/// canonical `bd_tokens` scanner so boundaries match the bead-hunk guard.
pub fn extract_bead_id(message: &str) -> Option<String> {
    bd_tokens(message).into_iter().next()
}

/// Append one GitFact JSONL line to `journal_path`. Best-effort: `create_dir_all`
/// and append; ANY error is swallowed and never propagates (the commit must not
/// fail because the journal is unwritable).
pub fn append_git_fact_to_path(
    journal_path: &Path,
    repo: &str,
    sha: &str,
    subject: &str,
    bead_id: Option<&str>,
    pin: &str,
) {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "type".to_string(),
        serde_json::Value::String("git_fact".to_string()),
    );
    obj.insert(
        "repo".to_string(),
        serde_json::Value::String(repo.to_string()),
    );
    obj.insert(
        "sha".to_string(),
        serde_json::Value::String(sha.to_string()),
    );
    obj.insert(
        "subject".to_string(),
        serde_json::Value::String(subject.to_string()),
    );
    if let Some(bid) = bead_id {
        obj.insert(
            "bead_id".to_string(),
            serde_json::Value::String(bid.to_string()),
        );
    }
    obj.insert(
        "pin".to_string(),
        serde_json::Value::String(pin.to_string()),
    );
    let line = serde_json::Value::Object(obj).to_string();
    // Best-effort: never bubble errors.
    let _ = (|| -> anyhow::Result<()> {
        if let Some(parent) = journal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(journal_path)?;
        writeln!(file, "{line}")?;
        file.flush()?;
        Ok(())
    })();
}

/// Production append: writes to the canonical journal path.
pub fn append_git_fact(repo: &str, sha: &str, subject: &str, bead_id: Option<&str>, pin: &str) {
    append_git_fact_to_path(&git_fact_journal_path(), repo, sha, subject, bead_id, pin);
}

/// Record a GitFact for `HEAD` of `repo_path` attributed to `pin`.
/// Opens the repo, reads HEAD commit SHA + message, derives `repo` basename,
/// `subject` (first line), and `bead_id` via [`extract_bead_id`], then
/// appends via [`append_git_fact`] (best-effort, never errors).
pub fn record_git_fact_for_head(repo_path: &Path, pin: &str) {
    record_git_fact_for_head_with_journal(repo_path, pin, &git_fact_journal_path());
}

/// Testable variant with explicit journal path (hermetic tests set `XDG_STATE_HOME`
/// or pass a temp path). Best-effort — never returns an error, never panics.
pub fn record_git_fact_for_head_with_journal(repo_path: &Path, pin: &str, journal_path: &Path) {
    let Ok(repo) = git2::Repository::open(repo_path) else {
        return;
    };
    let Ok(head) = repo.head() else {
        return;
    };
    let Ok(commit) = head.peel_to_commit() else {
        return;
    };
    let sha = commit.id().to_string();
    let message = commit.message().unwrap_or_default().to_string();
    let subject = message.lines().next().unwrap_or_default().to_string();
    let bead_id = extract_bead_id(&message);
    let repo_name = repo_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown")
        .to_string();
    append_git_fact_to_path(
        journal_path,
        &repo_name,
        &sha,
        &subject,
        bead_id.as_deref(),
        pin,
    );
}

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

    // ---- bd-guard-crossproject-lease-er38: cross-project lease spelling ----

    #[test]
    fn er38_translates_sibling_repo_spelling() {
        assert_eq!(
            cross_project_pattern("../flywheel/src/x.rs", "flywheel"),
            Some("src/x.rs".to_string())
        );
        // Whole-repo lease (with or without trailing slash) covers all of it.
        assert_eq!(
            cross_project_pattern("../flywheel", "flywheel"),
            Some("**".to_string())
        );
        assert_eq!(
            cross_project_pattern("../flywheel/", "flywheel"),
            Some("**".to_string())
        );
    }

    #[test]
    fn er38_rejects_non_matching_shapes() {
        // A different sibling never translates here.
        assert_eq!(cross_project_pattern("../other/src/x.rs", "flywheel"), None);
        // Repo-relative spellings are not escapes; caller keeps them as-is.
        assert_eq!(cross_project_pattern("src/x.rs", "flywheel"), None);
        // Deep escapes and backslash spellings are never guessed.
        assert_eq!(cross_project_pattern("../../x.rs", "flywheel"), None);
        assert_eq!(
            cross_project_pattern("..\\flywheel\\x.rs", "flywheel"),
            None
        );
    }

    #[test]
    fn er38_toron_namespaced_lease_satisfies_flywheel_guard() {
        // The regression: a lease taken on project toron as
        // `../flywheel/src/x.rs` must cover staged `src/x.rs` when the
        // commit happens in the flywheel repo — with NO second lease.
        let raw = res("CrimsonIsland", "../flywheel/src/x.rs", true);
        let translated = cross_project_pattern(&raw.pattern, "flywheel").expect("translate");
        let own = vec![res("CrimsonIsland", &translated, true)];
        let staged = vec!["src/x.rs".to_string()];
        let blocks = guard_check_strict(Some("CrimsonIsland"), &staged, &own, SHARED_SAFE_PATHS);
        assert!(blocks.is_empty(), "one lease must cover: {blocks:?}");

        // Symmetric fairness: the same spelling held by ANOTHER agent blocks.
        let foreign = vec![res("TealDune", &translated, true)];
        let blocks =
            guard_check_strict(Some("CrimsonIsland"), &staged, &foreign, SHARED_SAFE_PATHS);
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].kind, StrictBlockKind::HeldBy("TealDune".into()));

        // Without translation the guard saw Unreserved — the defect.
        let untranslated = vec![res("CrimsonIsland", "../flywheel/src/x.rs", true)];
        let blocks = guard_check_strict(
            Some("CrimsonIsland"),
            &staged,
            &untranslated,
            SHARED_SAFE_PATHS,
        );
        assert_eq!(blocks[0].kind, StrictBlockKind::Unreserved);
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

    // -----------------------------------------------------------------------
    // bd-9mb8: bead-hunk attribution — bd-* tokens ADDED by the staged diff
    // must appear in the commit message, else the commit is attributed to
    // nobody for that work.
    // -----------------------------------------------------------------------

    #[test]
    fn bead_hunks_foreign_token_blocked() {
        let diff = "+// governed-by: bd-foreign-abc\n+pub fn x() {}\n";
        let v = bead_hunk_violations(diff, "feat: stuff without any ids");
        assert_eq!(
            v,
            vec![BeadHunkBlock {
                token: "bd-foreign-abc".to_string()
            }]
        );
    }

    #[test]
    fn bead_hunks_token_present_in_message_allowed() {
        let diff = "+// governed-by: bd-9mb8\n";
        let v = bead_hunk_violations(diff, "fix(guard): attribution bd-9mb8");
        assert!(v.is_empty(), "a token carried by the message is attributed");
    }

    #[test]
    fn bead_hunks_removed_and_context_lines_carry_no_debt() {
        let diff = "-// governed-by: bd-old-one\n // governed-by: bd-ctx-nine\n";
        let v = bead_hunk_violations(diff, "chore: cleanup");
        assert!(
            v.is_empty(),
            "only ADDED lines introduce attribution debt, got {v:?}"
        );
    }

    #[test]
    fn bead_hunks_unified_headers_ignored() {
        let diff = "--- a/src/x.rs\n+++ b/src/x.rs\n";
        assert!(bead_hunk_violations(diff, "").is_empty());
    }

    #[test]
    fn bead_hunks_multiple_tokens_deduped() {
        let diff = "+// bd-alpha-one\n+// bd-alpha-one again, plus bd-beta-two\n";
        let mut v = bead_hunk_violations(diff, "");
        v.sort_by(|a, b| a.token.cmp(&b.token));
        assert_eq!(
            v,
            vec![
                BeadHunkBlock {
                    token: "bd-alpha-one".to_string()
                },
                BeadHunkBlock {
                    token: "bd-beta-two".to_string()
                },
            ]
        );
    }

    #[test]
    fn bd_tokens_shape_and_boundaries() {
        assert_eq!(bd_tokens("// bd-9mb8 ok"), vec!["bd-9mb8".to_string()]);
        assert_eq!(
            bd_tokens("(bd-adr22-w6-stage)"),
            vec!["bd-adr22-w6-stage".to_string()]
        );
        assert_eq!(bd_tokens("bd-a-- rest"), vec!["bd-a".to_string()]);
        assert_eq!(bd_tokens("abd-9 not a boundary"), Vec::<String>::new());
        assert_eq!(bd_tokens("x-bd-hidden flag form"), Vec::<String>::new());
        assert_eq!(bd_tokens("BD-UPPER case sensitive"), Vec::<String>::new());
        assert_eq!(bd_tokens("bd- lone prefix"), Vec::<String>::new());
        assert_eq!(bd_tokens("bd-a bd-a dedup"), vec!["bd-a".to_string()]);
    }

    #[test]
    fn staged_added_lines_exclude_shared_safe_paths() {
        let root = std::env::temp_dir().join(format!(
            "toron-added-lines-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(root.join("src")).expect("src dir");
        std::fs::create_dir_all(root.join(".beads")).expect("beads dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        std::fs::write(
            root.join("src/lib.rs"),
            "// governed-by: bd-hunk-code\npub fn x() {}\n",
        )
        .expect("write lib");
        std::fs::write(
            root.join(".beads/issues.jsonl"),
            "{\"id\":\"bd-ledger-only\"}\n",
        )
        .expect("write ledger");

        let mut index = repo.index().expect("index");
        index.add_path(Path::new("src/lib.rs")).expect("add lib");
        index
            .add_path(Path::new(".beads/issues.jsonl"))
            .expect("add ledger");
        index.write().expect("write index");

        let added = staged_added_lines_in(&root, SHARED_SAFE_PATHS);
        assert!(
            added.contains("bd-hunk-code"),
            "code hunks feed the scan, got: {added}"
        );
        assert!(
            !added.contains("bd-ledger-only"),
            ".beads ledger syncs legitimately carry foreign ids and must be exempt, got: {added}"
        );
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn install_writes_commit_msg_hook_and_uninstall_removes_it() {
        let root = std::env::temp_dir().join(format!(
            "toron-commit-msg-hook-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");

        install_precommit_guard(&root, "toron").expect("install");
        let cm = repo.path().join("hooks").join("commit-msg");
        let contents = std::fs::read_to_string(&cm).expect("commit-msg written by install");
        assert!(
            contents.contains("--msg-file \"$1\""),
            "commit-msg hook must pass the message file: {contents}"
        );
        assert!(
            contents.contains("installed by toron"),
            "commit-msg carries the toron marker"
        );

        assert!(uninstall_precommit_guard(&root).expect("uninstall"));
        assert!(!cm.exists(), "our commit-msg hook removed on uninstall");
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn install_never_clobbers_foreign_commit_msg_hook() {
        let root = std::env::temp_dir().join(format!(
            "toron-clobber-commit-msg-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let hooks = repo.path().join("hooks");
        std::fs::create_dir_all(&hooks).expect("hooks dir");
        std::fs::write(hooks.join("commit-msg"), "#!/bin/sh\n# user's own\n").expect("write");

        install_precommit_guard(&root, "demo").expect("install");
        let cm = std::fs::read_to_string(hooks.join("commit-msg")).expect("read");
        assert!(cm.contains("user's own"), "foreign commit-msg untouched");

        // ...and uninstall must NOT delete the user's hook either.
        assert!(uninstall_precommit_guard(&root).expect("uninstall"));
        assert!(
            hooks.join("commit-msg").exists(),
            "user's commit-msg survives uninstall"
        );
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    // -----------------------------------------------------------------------
    // bd-jhes GitFact producer
    // -----------------------------------------------------------------------

    #[test]
    fn git_fact_line_shape_with_bead_id() {
        let dir = std::env::temp_dir().join(format!(
            "toron-git-fact-shape-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&dir);
        append_git_fact_to_path(
            &dir,
            "toron",
            "abc123deadbeef",
            "fix: thing [bd-jhes]",
            Some("bd-jhes"),
            "GreenPine",
        );
        let content = std::fs::read_to_string(&dir).expect("journal written");
        let line = content.trim();
        let v: serde_json::Value = serde_json::from_str(line).expect("valid json");
        let obj = v.as_object().expect("object");
        assert_eq!(obj.get("type").unwrap(), "git_fact");
        assert_eq!(obj.get("repo").unwrap(), "toron");
        assert_eq!(obj.get("sha").unwrap(), "abc123deadbeef");
        assert_eq!(obj.get("subject").unwrap(), "fix: thing [bd-jhes]");
        assert_eq!(obj.get("bead_id").unwrap(), "bd-jhes");
        assert_eq!(obj.get("pin").unwrap(), "GreenPine");
        assert_eq!(obj.len(), 6, "exactly those keys when bead_id present");
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn git_fact_line_shape_without_bead_id() {
        let dir = std::env::temp_dir().join(format!(
            "toron-git-fact-nobead-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&dir);
        append_git_fact_to_path(&dir, "flywheel", "fff", "chore: cleanup", None, "BlueLake");
        let content = std::fs::read_to_string(&dir).expect("journal written");
        let v: serde_json::Value = serde_json::from_str(content.trim()).expect("valid json");
        let obj = v.as_object().expect("object");
        assert_eq!(obj.get("type").unwrap(), "git_fact");
        assert!(!obj.contains_key("bead_id"), "bead_id omitted when absent");
        assert_eq!(obj.len(), 5, "exactly 5 keys when no bead_id");
        let _ = std::fs::remove_file(&dir);
    }

    #[test]
    fn git_fact_extract_bead_id_variants() {
        assert_eq!(
            extract_bead_id("fix: thing [bd-jhes]"),
            Some("bd-jhes".to_string())
        );
        assert_eq!(
            extract_bead_id("feat: add login [bd-abc123] extra"),
            Some("bd-abc123".to_string())
        );
        assert_eq!(extract_bead_id("no bead here"), None);
        assert_eq!(
            extract_bead_id("multiple bd-first-one and bd-second-two"),
            Some("bd-first-one".to_string())
        );
        // boundary check: x-bd-hidden should not match
        assert_eq!(extract_bead_id("x-bd-hidden"), None);
    }

    #[test]
    fn git_fact_journal_write_failure_does_not_panic() {
        // Simulate unwritable journal by making the journal_path a directory.
        let dir = std::env::temp_dir().join(format!(
            "toron-git-fact-unwritable-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).expect("create dir");
        // Using the directory itself as the file path must not panic and must not block.
        append_git_fact_to_path(&dir, "toron", "deadbeef", "subject", None, "Pin");
        // Also test record helper with unwritable journal — must not panic.
        let repo_dir = std::env::temp_dir().join(format!(
            "toron-git-fact-repo-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&repo_dir).expect("repo parent");
        let repo = git2::Repository::init(&repo_dir).expect("init repo");
        // create an initial commit so HEAD exists
        std::fs::write(repo_dir.join("a.txt"), "hello\n").expect("write");
        let mut index = repo.index().expect("index");
        index.add_path(Path::new("a.txt")).expect("add");
        index.write().expect("write index");
        let tree_id = index.write_tree().expect("tree");
        let tree = repo.find_tree(tree_id).expect("tree");
        let sig = git2::Signature::now("Test", "test@test.com").expect("sig");
        repo.commit(Some("HEAD"), &sig, &sig, "init commit", &tree, &[])
            .expect("commit");
        drop(tree);
        // record with the directory-as-file must not panic (best-effort)
        record_git_fact_for_head_with_journal(&repo_dir, "TestPin", &dir);
        drop(repo);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&repo_dir);
    }

    #[test]
    fn git_fact_record_writes_one_jsonl_line() {
        let repo_dir = std::env::temp_dir().join(format!(
            "toron-git-fact-record-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&repo_dir).expect("repo parent");
        let repo = git2::Repository::init(&repo_dir).expect("init repo");
        std::fs::write(repo_dir.join("b.txt"), "world\n").expect("write");
        let mut index = repo.index().expect("index");
        index.add_path(Path::new("b.txt")).expect("add");
        index.write().expect("write index");
        let tree_id = index.write_tree().expect("tree");
        let tree = repo.find_tree(tree_id).expect("tree");
        let sig = git2::Signature::now("Test", "test@test.com").expect("sig");
        let oid = repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "feat: add thing [bd-jhes]\n\nbody",
                &tree,
                &[],
            )
            .expect("commit");
        drop(tree);
        let journal = std::env::temp_dir().join(format!(
            "toron-git-fact-journal-{}-{}.jsonl",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&journal);
        record_git_fact_for_head_with_journal(&repo_dir, "IndigoCove", &journal);
        let content = std::fs::read_to_string(&journal).expect("journal written");
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines.len(), 1, "ONE JSONL line");
        let v: serde_json::Value = serde_json::from_str(lines[0]).expect("valid json");
        assert_eq!(v["type"], "git_fact");
        assert_eq!(v["repo"], repo_dir.file_name().unwrap().to_str().unwrap());
        assert_eq!(v["sha"], oid.to_string());
        assert_eq!(v["subject"], "feat: add thing [bd-jhes]");
        assert_eq!(v["bead_id"], "bd-jhes");
        assert_eq!(v["pin"], "IndigoCove");
        drop(repo);
        let _ = std::fs::remove_file(&journal);
        let _ = std::fs::remove_dir_all(&repo_dir);
    }

    #[test]
    fn git_fact_journal_path_respects_xdg_state_home() {
        let tmp = std::path::Path::new("/tmp/xdg-test-abc");
        let p = git_fact_journal_path_with_xdg(Some(tmp));
        assert_eq!(p, tmp.join("flywheel/journals/git-facts.jsonl"));
        let p2 = git_fact_journal_path_with_xdg(None);
        assert_eq!(
            p2,
            std::path::PathBuf::from(
                "/Users/bangedorrunt/.local/state/flywheel/journals/git-facts.jsonl"
            )
        );
    }

    #[test]
    fn install_writes_post_commit_hook_and_uninstall_removes_it() {
        let root = std::env::temp_dir().join(format!(
            "toron-post-commit-hook-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");

        install_precommit_guard(&root, "toron").expect("install");
        let pc = repo.path().join("hooks").join("post-commit");
        let contents = std::fs::read_to_string(&pc).expect("post-commit written");
        assert!(
            contents.contains("guard record"),
            "post-commit must call guard record: {contents}"
        );
        assert!(
            contents.contains("installed by toron"),
            "post-commit carries marker"
        );
        assert!(
            contents.contains("|| true"),
            "post-commit is best-effort (|| true)"
        );

        assert!(uninstall_precommit_guard(&root).expect("uninstall"));
        assert!(!pc.exists(), "post-commit removed on uninstall");
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn install_never_clobbers_foreign_post_commit_hook() {
        let root = std::env::temp_dir().join(format!(
            "toron-clobber-post-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&root).expect("create repo dir");
        let repo = git2::Repository::init(&root).expect("init repo");
        let hooks = repo.path().join("hooks");
        std::fs::create_dir_all(&hooks).expect("hooks dir");
        std::fs::write(hooks.join("post-commit"), "#!/bin/sh\n# user's own post\n").expect("write");

        install_precommit_guard(&root, "demo").expect("install");
        let cm = std::fs::read_to_string(hooks.join("post-commit")).expect("read");
        assert!(
            cm.contains("user's own post"),
            "foreign post-commit untouched"
        );

        assert!(uninstall_precommit_guard(&root).expect("uninstall"));
        assert!(
            hooks.join("post-commit").exists(),
            "user's post-commit survives uninstall"
        );
        drop(repo);
        let _ = std::fs::remove_dir_all(root);
    }
}
