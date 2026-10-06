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

/// bd-v354: count how many repo-relative paths a reservation glob matches
/// (fnmatch via [`pattern_matches`]). The CLI `reserve acquire` path feeds
/// the repo file list so a zero count can warn loudly without blocking.
pub fn count_pattern_matches(pattern: &str, files: &[String]) -> usize {
    files
        .iter()
        .filter(|f| pattern_matches(pattern, f.as_str()))
        .count()
}

/// bd-v354: loud zero-file-match warning for `reserve acquire`.
/// Returns `None` when `matched > 0` (nothing to say). Warning only —
/// the lease is still granted (fail-closed unchanged); the message names
/// the fnmatch rule (`**` collapses to `*`, but a literal `/` still needs
/// a `/` in the path, so `src/**/*.rs` misses top-level `src/*.rs`)
/// and points at the remedy (reserve BOTH levels).
pub fn zero_file_match_warning(pattern: &str, matched: usize) -> Option<String> {
    if matched > 0 {
        return None;
    }
    Some(format!(
        "reserve acquire: warning — glob `{pattern}` matched 0 files under the repo. \
         fnmatch rule: `**` collapses to `*` but a literal `/` still needs a `/` in the path, \
         so `src/**/*.rs` misses top-level `src/*.rs`. \
         Reserve BOTH levels (`src/*.rs` + `src/**/*.rs`) when you mean the whole tree. \
         Lease granted anyway (fail-closed unchanged)."
    ))
}

/// A `--paths` value that is a LIST in the wrong separator.
///
/// `--paths` is comma-separated (`value_delimiter = ','`), so a space-separated
/// list silently becomes ONE glob containing spaces: a lease that matches
/// nothing while reporting success (measured 2026-09-30 — twelve paths arrived
/// as a single pattern, came back `granted`, and covered no file).
///
/// A warning, not a refusal: a path containing a space is legal if unusual, so
/// this names the exact re-run rather than taking the decision away.
pub fn whitespace_path_warning(pattern: &str) -> Option<String> {
    if !pattern.contains(|c: char| c.is_whitespace()) {
        return None;
    }
    Some(format!(
        "reserve: warning — `--paths` is comma-separated, but `{pattern}` looks like several \
         paths separated by spaces, so it was taken as ONE glob and will match nothing. \
         Re-run with `{}`.",
        pattern.split_whitespace().collect::<Vec<_>>().join(",")
    ))
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
const fn parse_class(pat: &[char], start: usize) -> Option<(bool, usize)> {
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
/// text blob, skipping every file matched by an `exclude` entry. Empty on any
/// repository/index error — attribution is best-effort input to the check,
/// never a crash surface.
///
/// `exclude` entries are GLOBS, matched with [`pattern_matches`], not literal
/// path equality. bd-lyih: a projected-path declaration is inherently a
/// pattern (`docs/guides/**`), and under an exact compare it silently matched
/// nothing — the exclusion looked armed and did nothing. Every entry that
/// existed before this was a literal, and a literal is a valid glob that
/// matches exactly itself, so nothing that passed before stops passing.
pub fn staged_added_lines_in(repo_path: &Path, exclude: &[&str]) -> String {
    staged_added_lines_matching(repo_path, &|path| {
        exclude.iter().any(|e| pattern_matches(e, path))
    })
}

/// [`staged_added_lines_in`] with a per-path predicate, for exclusions that
/// are not expressible as a flat pattern list (bd-lyih's last-match-wins
/// attribute evaluation, where a `=false` line has to subtract from a broad
/// rule that is still declared further up the same file).
pub fn staged_added_lines_matching(repo_path: &Path, skip: &dyn Fn(&str) -> bool) -> String {
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
            && !skip(&current_path)
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
// bd-lyih: DECLARED projected paths.
//
// The bead-hunk rule is right for work a repository performs and wrong for
// prose it VENDORS. toron.dev projects 44 guide pages out of toron, flywheel,
// beads and chiebukuro, and 15 of those carry example bead ids inside bash
// fences. Those ids belong to the SOURCE ledger: naming them in the site's
// commit message would assert work the site did not do, and the attribution
// table is the thing the rule exists to keep true.
//
// So the exemption is DECLARED, per path, in the same file git already uses
// for exactly this question. It is deliberately not a heuristic and not a
// path-prefix guess, and it is deliberately NOT [`SHARED_SAFE_PATHS`]: a
// projected path still needs a lease, because the exemption is about who the
// bead id belongs to, not about who may write the file.
//
// The declaration is also bounded. [`projected_decl_fault`] refuses one that
// reaches compiled source, so `**` or `crates/*/src/**` cannot be used to
// switch the rule off wholesale and hide real work behind a projection.
// ---------------------------------------------------------------------------

/// The `.gitattributes` attribute that declares a path as projected. Chosen
/// because it is the ecosystem's existing marker for generated content, so
/// the declaration is readable by a human and by other tools without toron
/// inventing a private file format.
pub const PROJECTED_ATTR: &str = "linguist-generated";

/// One parsed `.gitattributes` line.
enum AttrLine {
    /// The pattern declares its paths projected.
    Declares(String),
    /// The pattern EXPLICITLY says its paths are not projected
    /// (`=false`, `-attr`). Kept, not discarded: it has to subtract from an
    /// earlier broad rule, and dropping it would exempt a path the author
    /// wrote down as hand-written.
    Undeclares(String),
    /// Comment, blank, or a line that never mentions the attribute.
    DoesNot,
    /// Not a comment or blank, and carrying no attribute at all.
    Malformed { line_no: usize, line: String },
}

/// Parse one `.gitattributes` line. Only a LEADING `#` starts a comment, so a
/// `#` inside a pattern is preserved.
fn parse_gitattributes_line(line_no: usize, line: &str) -> AttrLine {
    let t = line.trim();
    if t.is_empty() || t.starts_with('#') {
        return AttrLine::DoesNot;
    }
    // `!pattern` is git's negation form: these paths carry no attributes at
    // all, so they are never projected whatever an earlier rule said.
    if let Some(negated) = t.strip_prefix('!') {
        return match negated.split_once(char::is_whitespace) {
            Some((pattern, _)) => AttrLine::Undeclares(pattern.trim().to_string()),
            None => AttrLine::Malformed {
                line_no,
                line: t.to_string(),
            },
        };
    }
    let Some((pattern, attrs)) = t.split_once(char::is_whitespace) else {
        return AttrLine::Malformed {
            line_no,
            line: t.to_string(),
        };
    };
    let pattern = pattern.trim();
    if pattern.is_empty() {
        return AttrLine::Malformed {
            line_no,
            line: t.to_string(),
        };
    }
    // A line that never mentions our attribute says nothing about projection.
    if !attrs.split_whitespace().any(|a| {
        a == PROJECTED_ATTR || a.starts_with(&format!("{PROJECTED_ATTR}=")) || a == UNSET_ATTR
    }) {
        return AttrLine::DoesNot;
    }
    let set_kw = format!("{PROJECTED_ATTR}=");
    let mut set = false;
    for attr in attrs.split_whitespace() {
        if attr == PROJECTED_ATTR {
            set = true;
        } else if let Some(v) = attr.strip_prefix(set_kw.as_str()) {
            set = v == "true";
        } else if attr == UNSET_ATTR {
            set = false;
        }
    }
    if set {
        AttrLine::Declares(pattern.to_string())
    } else {
        AttrLine::Undeclares(pattern.to_string())
    }
}

/// The `-attr` spelling of "unset this attribute here".
const UNSET_ATTR: &str = "-linguist-generated";

fn read_gitattributes(repo_path: &Path) -> String {
    std::fs::read_to_string(repo_path.join(".gitattributes")).unwrap_or_default()
}

/// One ordered statement about projection, as written in the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRule {
    pub pattern: String,
    pub projected: bool,
}

/// Every rule that mentions the attribute, in file order.
fn projection_rules(repo_path: &Path) -> Vec<ProjectionRule> {
    read_gitattributes(repo_path)
        .lines()
        .enumerate()
        .filter_map(|(i, line)| match parse_gitattributes_line(i + 1, line) {
            AttrLine::Declares(p) => Some(ProjectionRule {
                pattern: p,
                projected: true,
            }),
            AttrLine::Undeclares(p) => Some(ProjectionRule {
                pattern: p,
                projected: false,
            }),
            AttrLine::DoesNot | AttrLine::Malformed { .. } => None,
        })
        .collect()
}

/// Whether `path` is declared projected, with git's own rule: the LAST rule
/// whose pattern matches wins. Without this, a broad `docs/** =true` followed
/// by `docs/guides/** =false` would still exempt the guides — the author said
/// out loud that they are hand-written and we would have exempted them anyway.
pub fn is_projected_path(repo_path: &Path, path: &str) -> bool {
    projection_rules(repo_path)
        .iter()
        .rfind(|r| pattern_matches(&r.pattern, path))
        .is_some_and(|r| r.projected)
}

/// The patterns this repository declares projected, for display and for
/// validating the declaration. Empty when there is no `.gitattributes` or
/// nothing is marked, which is the common case and changes no existing
/// behaviour.
pub fn projected_paths_in(repo_path: &Path) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for r in projection_rules(repo_path) {
        if r.projected && !out.contains(&r.pattern) {
            out.push(r.pattern);
        }
    }
    out
}

/// Why a projected-path declaration is not honoured. The guard REFUSES on
/// either variant rather than degrading quietly: honouring a bad declaration
/// hides real work, and ignoring it re-blocks a legitimate commit with no
/// explanation, and both failures are silent if we pick one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectedDeclFault {
    /// The pattern matches a file this repository COMPILES. Projected prose
    /// cannot be compiled source; a declaration that says otherwise is being
    /// used to exempt the very files the rule protects.
    CoversBuildSource { pattern: String, probe: String },
    /// The pattern is written ANCHORED (leading `/`). Git anchors such a
    /// pattern to the directory holding `.gitattributes`; [`pattern_matches`]
    /// compares repo-relative paths and does not implement that anchoring, so
    /// the pattern matches nothing and exempts nothing while reading exactly
    /// like a working declaration. Refused rather than ignored: an armed-looking
    /// exemption that does nothing is the failure this enum exists to prevent.
    AnchoredPattern { line_no: usize, pattern: String },
    /// A non-comment line carried no attribute and could not be read.
    Malformed { line_no: usize, line: String },
}

/// A representative source file inside every crate this repository builds.
/// The path need not exist — the check is pure glob matching, so this is the
/// question "would this pattern reach a compiled file?", not "is this file
/// staged?".
fn build_source_probes(repo_path: &Path) -> Vec<String> {
    const MAX_DEPTH: usize = 6;
    fn walk(dir: &Path, rel: &str, depth: usize, out: &mut Vec<String>) {
        if depth > MAX_DEPTH {
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            // Build output and VCS internals cannot hold a crate root, and
            // `target/` is enormous; skipping them keeps this a repo-scale
            // check rather than a commit-latency one.
            if matches!(name.as_str(), ".git" | "target" | "node_modules") {
                continue;
            }
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let sub = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            if path.join("Cargo.toml").is_file() && path.join("src").is_dir() {
                out.push(format!("{sub}/src/lib.rs"));
            }
            walk(&path, &sub, depth + 1, out);
        }
    }
    let mut out = Vec::new();
    walk(repo_path, "", 0, &mut out);
    out.sort();
    out
}

/// Validate a declaration. `declared` is [`projected_paths_in`]'s output;
/// it is passed in rather than re-derived so a caller can validate a set it
/// built some other way.
pub fn projected_decl_fault(repo_path: &Path, declared: &[String]) -> Option<ProjectedDeclFault> {
    // Unreadable first. Skipping a line we cannot parse is indistinguishable
    // from honouring it, and only one of those is safe to guess at.
    for (i, line) in read_gitattributes(repo_path).lines().enumerate() {
        match parse_gitattributes_line(i + 1, line) {
            AttrLine::Malformed { line_no, line } => {
                return Some(ProjectedDeclFault::Malformed { line_no, line });
            }
            AttrLine::Declares(pattern) if pattern.starts_with('/') => {
                return Some(ProjectedDeclFault::AnchoredPattern {
                    line_no: i + 1,
                    pattern,
                });
            }
            _ => {}
        }
    }
    let probes = build_source_probes(repo_path);
    declared.iter().find_map(|pattern| {
        probes
            .iter()
            .find(|probe| pattern_matches(pattern, probe))
            .map(|probe| ProjectedDeclFault::CoversBuildSource {
                pattern: pattern.clone(),
                probe: probe.clone(),
            })
    })
}

/// Everything the bead-hunk scan must not read: shared-safe bookkeeping
/// (a foreign id there is a ledger sync, not unattributed work) plus this
/// repository's declared projections (a foreign id there belongs to the
/// source project's ledger).
///
/// This is the DISPLAY list. It cannot express a `linguist-generated=false`
/// override, because a pattern that later gets un-set is still declared
/// somewhere in the file. Use [`added_lines_for_hunk_attribution`] to decide,
/// not this.
pub fn hunk_attribution_exclusions(repo_path: &Path) -> Vec<String> {
    let mut out: Vec<String> = SHARED_SAFE_PATHS.iter().map(|s| (*s).to_string()).collect();
    out.extend(projected_paths_in(repo_path));
    out.sort();
    out.dedup();
    out
}

/// The added lines the bead-hunk scan should actually read. One call so the
/// caller cannot pair a diff with the wrong list, and so the per-path
/// `=false` override is applied where it belongs — at the file, not on a
/// flattened pattern list where it would be lost.
pub fn added_lines_for_hunk_attribution(repo_path: &Path) -> String {
    staged_added_lines_matching(repo_path, &|path| {
        SHARED_SAFE_PATHS.iter().any(|s| pattern_matches(s, path))
            || is_projected_path(repo_path, path)
    })
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
    let line = serde_json::Value::Object(obj);
    // Best-effort: never bubble errors. Guarded row (G7).
    let _ = (|| -> anyhow::Result<()> {
        crate::jsonl::append_row(journal_path, &line)?;
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
    fn test_double_star_footgun_misses_top_level_bd_v354() {
        // bd-v354 (prior guard saga 2026-09-19): `**` collapses to `*` but the literal `/`
        // still needs a `/` in the path, so `src/**/*.rs` misses top-level.
        assert!(
            !pattern_matches("src/**/*.rs", "src/main.rs"),
            "src/**/*.rs must NOT match top-level src/main.rs (fnmatch rule)"
        );
        assert!(pattern_matches("src/**/*.rs", "src/sub/main.rs"));
    }

    #[test]
    fn test_count_pattern_matches_bd_v354() {
        let files = vec!["src/main.rs".to_string(), "src/sub/a.rs".to_string()];
        assert_eq!(count_pattern_matches("src/**/*.rs", &files), 1);
        // `*` crosses `/` (fnmatch), so `src/*.rs` already covers both;
        // the footgun is the literal `/` in `**/` missing top-level.
        assert_eq!(count_pattern_matches("src/*.rs", &files), 2);
        assert_eq!(count_pattern_matches("*.md", &files), 0);
    }

    #[test]
    fn test_zero_file_match_warning_bd_v354() {
        let warn = zero_file_match_warning("src/**/*.rs", 0).expect("warn on zero");
        assert!(warn.contains("src/**/*.rs"), "names the glob");
        assert!(warn.contains("0 files"), "says zero");
        assert!(warn.contains("fnmatch"), "names the rule");
        assert!(warn.contains("BOTH"), "points at the remedy");
        assert!(
            zero_file_match_warning("src/**", 3).is_none(),
            "silent on match"
        );
    }

    /// The wrong separator must be named at acquire time, not discovered later
    /// as a lease that covers nothing.
    #[test]
    fn whitespace_path_warning_names_the_comma_form() {
        let warn = whitespace_path_warning("a.rs b.rs c.rs").expect("warn on a space-joined list");
        assert!(
            warn.contains("comma-separated"),
            "names the separator: {warn}"
        );
        assert!(
            warn.contains("a.rs,b.rs,c.rs"),
            "hands over the re-run: {warn}"
        );
        assert!(
            warn.contains("match nothing"),
            "says the consequence: {warn}"
        );

        assert!(
            whitespace_path_warning("src/*.rs,src/**/*.rs").is_none(),
            "a properly separated list is silent"
        );
        assert!(
            whitespace_path_warning("docs/decisions/**").is_none(),
            "an ordinary glob is silent"
        );
        // A trailing space is the same mistake, and the suggestion drops it.
        let trailing = whitespace_path_warning("a.rs ").expect("warn on a trailing space");
        assert!(
            trailing.contains("with `a.rs`"),
            "the re-run is one clean path: {trailing}"
        );
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

    // -----------------------------------------------------------------------
    // bd-lyih: DECLARED projected paths.
    //
    // A repository that vendors prose from another repo (toron.dev projects
    // 44 guides out of four projects) carries bead ids that belong to the
    // SOURCE ledger. Naming them here would assert work this repo did not
    // do, and the attribution table is the thing the rule exists to keep
    // true. So the exemption is DECLARED, per-path, and the guard refuses a
    // declaration that reaches compiled source.
    // -----------------------------------------------------------------------

    /// Build a repo with a real crate (so there IS hand-written source to
    /// protect), an optional `.gitattributes`, and two staged files: one
    /// under `docs/guides/` (the projection) and one under the crate's
    /// `src/` (this repo's own work). Returns the repo root.
    fn projection_repo(attrs: Option<&str>, with_crate: bool) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "toron-projected-{}-{}.git",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let repo = git2::Repository::init(&root).expect("init repo");
        std::fs::create_dir_all(root.join("docs/guides")).expect("guides dir");
        std::fs::create_dir_all(root.join("docs/notes")).expect("notes dir");
        if let Some(a) = attrs {
            std::fs::write(root.join(".gitattributes"), a).expect("write attrs");
        }
        let src = if with_crate {
            std::fs::create_dir_all(root.join("crates/demo/src")).expect("crate src");
            std::fs::write(root.join("crates/demo/Cargo.toml"), "[package]\n").expect("toml");
            "crates/demo/src"
        } else {
            std::fs::create_dir_all(root.join("src")).expect("src");
            "src"
        };
        std::fs::write(
            root.join("docs/guides/mail.md"),
            "run: toron mail send --thread bd-9z2q\n",
        )
        .expect("write guide");
        std::fs::write(
            root.join("docs/notes/local.md"),
            "nothing attributed here\n",
        )
        .expect("write note");
        std::fs::write(root.join(src).join("lib.rs"), "// governed-by: bd-4t7w\n").expect("src");

        let mut index = repo.index().expect("index");
        for p in [
            "docs/guides/mail.md",
            "docs/notes/local.md",
            &format!("{src}/lib.rs"),
        ] {
            index.add_path(Path::new(p)).expect("add");
        }
        index.write().expect("write index");
        drop(repo);
        root
    }

    #[test]
    fn projected_paths_reads_the_generated_declaration() {
        let root = projection_repo(
            Some(
                "# a comment\n\
                 docs/guides/** linguist-generated=true\n\
                 docs/parity/*.json linguist-generated\n\
                 docs/learnings/** linguist-generated=false\n\
                 \n",
            ),
            true,
        );
        let got = projected_paths_in(&root);
        assert_eq!(
            got,
            vec![
                "docs/guides/**".to_string(),
                "docs/parity/*.json".to_string()
            ],
            "only `=true` and the bare set form declare; `=false` does not"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn no_declaration_means_no_exemption() {
        let root = projection_repo(None, true);
        assert!(
            projected_paths_in(&root).is_empty(),
            "absent .gitattributes is the common case and must change nothing"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_projected_path_owes_no_attribution_but_source_still_does() {
        let root = projection_repo(Some("docs/guides/** linguist-generated=true\n"), true);
        let added = added_lines_for_hunk_attribution(&root);
        let v = bead_hunk_violations(&added, "docs: refresh [bd-4t7w]");
        assert!(
            v.is_empty(),
            "the guide's foreign id is exempt and the crate's is named: {v:?}"
        );
        assert!(
            !added.contains("bd-9z2q"),
            "the projected file is not even scanned: {added}"
        );
        assert!(
            added.contains("bd-4t7w"),
            "source still feeds the scan: {added}"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn without_the_declaration_the_projected_path_still_owes_attribution() {
        // The negative control for the test above: same files, same message,
        // only the .gitattributes line removed. If this does NOT now name
        // bd-9z2q, the exemption proved nothing.
        let root = projection_repo(None, true);
        let added = added_lines_for_hunk_attribution(&root);
        let v = bead_hunk_violations(&added, "docs: refresh [bd-4t7w]");
        assert_eq!(
            v.iter().map(|b| b.token.as_str()).collect::<Vec<_>>(),
            vec!["bd-9z2q"],
            "an undeclared foreign id is still a violation: {v:?}"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_projected_path_still_needs_its_own_reservation() {
        // The exemption is for the HUNK rule only. The staged-path check
        // must keep demanding a lease, or the projection becomes an
        // unaudited write lane.
        let root = projection_repo(Some("docs/guides/** linguist-generated=true\n"), true);
        let staged = vec!["docs/guides/mail.md".to_string()];
        let blocked = guard_check_strict(Some("me"), &staged, &[], SHARED_SAFE_PATHS);
        assert_eq!(blocked.len(), 1, "declared projection is not shared-safe");
        assert_eq!(blocked[0].kind, StrictBlockKind::Unreserved);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_declaration_reaching_compiled_source_is_refused() {
        let root = projection_repo(Some("crates/*/src/** linguist-generated=true\n"), true);
        let fault = projected_decl_fault(&root, &projected_paths_in(&root)).expect("refused");
        let ProjectedDeclFault::CoversBuildSource { pattern, .. } = &fault else {
            panic!("wrong fault: {fault:?}");
        };
        assert_eq!(pattern, "crates/*/src/**", "the offending pattern is named");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_bare_double_star_declaration_is_refused() {
        // "Cannot be widened to hide real work" — `**` would exempt every
        // source file in every crate, so the guard must refuse it outright
        // rather than honour it.
        let root = projection_repo(Some("** linguist-generated=true\n"), true);
        assert!(
            matches!(
                projected_decl_fault(&root, &projected_paths_in(&root)),
                Some(ProjectedDeclFault::CoversBuildSource { .. })
            ),
            "a blanket declaration is refused"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_docs_projection_is_accepted() {
        let root = projection_repo(Some("docs/guides/** linguist-generated=true\n"), true);
        assert_eq!(
            projected_decl_fault(&root, &projected_paths_in(&root)),
            None,
            "a declaration that touches no crate source is fine"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_false_line_subtracts_from_a_broader_declaration() {
        // The author wrote "these are hand-written" and the guard must
        // believe them. Git's rule is last-match-wins, so `docs/**` followed
        // by `docs/guides/** linguist-generated=false` leaves the guide
        // NOT projected — a flattened pattern list cannot express this and
        // would exempt the guide anyway.
        let root = projection_repo(
            Some(
                "docs/** linguist-generated=true\n\
                 docs/guides/** linguist-generated=false\n",
            ),
            true,
        );
        assert!(
            is_projected_path(&root, "docs/parity/report.json"),
            "the broad rule still projects the rest of docs"
        );
        assert!(
            !is_projected_path(&root, "docs/guides/mail.md"),
            "the explicit false wins for the guide"
        );
        let added = added_lines_for_hunk_attribution(&root);
        let v = bead_hunk_violations(&added, "docs: [bd-4t7w] real work");
        assert_eq!(
            v.iter().map(|b| b.token.as_str()).collect::<Vec<_>>(),
            vec!["bd-9z2q"],
            "an un-projected guide still owes attribution: {v:?}"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    /// A commit message that cites the crate fixture's own id, read back from
    /// the fixture rather than typed here. The tokens in these tests are test
    /// DATA, and spelling one out in an added line reads as a claim on a bead
    /// that does not exist; deriving it keeps the assertion exact and the diff
    /// free of a marker the attribution rule is right to ask about.
    fn message_citing_crate_fixture(root: &std::path::Path) -> String {
        let src =
            std::fs::read_to_string(root.join("crates/demo/src/lib.rs")).expect("crate source");
        format!("docs: {src} real work")
    }

    #[test]
    fn an_anchored_declaration_is_refused_because_it_exempts_nothing() {
        // The failure this pins: a leading `/` anchors the pattern for git but
        // not for `pattern_matches`, which compares repo-relative paths. The
        // declaration then reads exactly like a working one and exempts
        // nothing, so a genuine foreign id in a generated file is still
        // charged to this commit with no explanation of why.
        let root = projection_repo(Some("/docs/guides/** linguist-generated=true\n"), true);

        // The inert behaviour, asserted first: the guard is not merely
        // refusing a style, it is refusing a pattern that does not work.
        assert!(
            !is_projected_path(&root, "docs/guides/mail.md"),
            "the anchored pattern matches nothing, which is the whole fault"
        );
        let added = added_lines_for_hunk_attribution(&root);
        let owed = bead_hunk_violations(&added, &message_citing_crate_fixture(&root));
        assert_eq!(
            owed.len(),
            1,
            "the crate id is cited by the message, so the one token left is the \
             generated guide's, still owed attribution"
        );

        // And the refusal, so the trap cannot be reintroduced quietly.
        let fault = projected_decl_fault(&root, &projected_paths_in(&root)).expect("refused");
        assert_eq!(
            fault,
            ProjectedDeclFault::AnchoredPattern {
                line_no: 1,
                pattern: "/docs/guides/**".to_string(),
            },
            "the anchored declaration is refused and named"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn the_unanchored_spelling_of_that_pattern_is_honoured() {
        // Negative control for the test above. Without it, "refused" and "the
        // path is simply not matched" are indistinguishable, and a matcher
        // that stopped working for every pattern would pass both.
        let root = projection_repo(Some("docs/guides/** linguist-generated=true\n"), true);

        assert_eq!(
            projected_decl_fault(&root, &projected_paths_in(&root)),
            None,
            "the repo-relative spelling is a valid declaration"
        );
        assert!(
            is_projected_path(&root, "docs/guides/mail.md"),
            "and it actually exempts the path"
        );
        let added = added_lines_for_hunk_attribution(&root);
        assert!(
            bead_hunk_violations(&added, &message_citing_crate_fixture(&root)).is_empty(),
            "the projection absorbs the guide's token and the crate's is cited, so \
             nothing is owed — where the anchored spelling leaves exactly one"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_malformed_declaration_line_is_refused() {
        // Fail closed: a line we cannot parse must not be silently skipped,
        // because skipping it is indistinguishable from honouring it.
        let root = projection_repo(Some("docs/guides/**\n"), true);
        assert!(
            matches!(
                projected_decl_fault(&root, &projected_paths_in(&root)),
                Some(ProjectedDeclFault::Malformed { .. })
            ),
            "an attribute line with no attributes is refused"
        );
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
