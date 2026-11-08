// governed-by: ADR-0003
//! `reserve with-lease` (bd-reserve-with-lease-2lvt): one scoped command.
//!
//! Acquire exclusive leases on `--paths`, run the command, ALWAYS release
//! (success, failure, signal), and exit with the command's own code. The
//! measured failure this verb ends: flips and small shared-file edits that
//! leave stale or phantom leases behind, or hold a shared ledger between
//! batches. One flip becomes one command with nothing leased after it.
//!
//! Two deliberate differences from `reserve acquire`:
//! - zero-file-match globs REFUSE here (acquire only warns): the wrapper must
//!   hold what it leases, so a lease over nothing is a refusal, not a warning;
//! - the acquire is always EXCLUSIVE: two scoped commands on one path must
//!   never overlap, so the second waits out the first's hold up to
//!   `--wait-secs` (0 = refuse on the first conflict) instead of interleaving.
//!
//! Lives outside the robot parity surface: the mailbox selector is the same
//! ([`MailboxSource`] — daemon when one serves, else the durable XDG root),
//! but the child process runs in the CLI's own process and cwd, which a
//! daemon cannot do.

use std::collections::BTreeSet;
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};
use toron_core::mailbox::{Mailbox, guard};
use toron_mcp::ToJson;

use crate::cli::ReserveWithLeaseArgs;
use crate::mailbox_source::MailboxSource;

/// Exit code for every with-lease REFUSAL (zero-match preflight, paths that
/// stay unacquirable): the command did not run and nothing was left leased.
/// Distinct from any child code so scripts can tell those apart.
pub(crate) const REFUSED: u8 = 3;

/// How often the wait loop re-attempts a path held by another pin.
const POLL: Duration = Duration::from_millis(250);

/// Grace for the child to die from the signal the terminal already delivered
/// to the foreground group before the wrapper escalates to SIGKILL.
const SIGNAL_GRACE: Duration = Duration::from_secs(2);

#[tokio::main]
pub async fn run_cli(args: &ReserveWithLeaseArgs, dry_run: bool) -> anyhow::Result<ExitCode> {
    Ok(ExitCode::from(run(args, dry_run).await?))
}

async fn run(args: &ReserveWithLeaseArgs, dry_run: bool) -> anyhow::Result<u8> {
    let paths = crate::robot::normalize_reserve_paths(&args.paths);
    for p in &paths {
        if let Some(w) = guard::whitespace_path_warning(p) {
            eprintln!("{w}");
        }
    }
    let files = crate::robot::repo_relative_files();
    if !files.is_empty() {
        for p in &paths {
            if let Some(refusal) = zero_match_refusal(p, &files, is_bare_dir(p)) {
                eprintln!("toron reserve with-lease: refused — {refusal}");
                print_json(&json!({
                    "acquired": false,
                    "refused": true,
                    "project": args.project,
                    "as": args.agent,
                    "paths": paths,
                    "reason": args.reason,
                    "refusal": refusal,
                    "exit_code": REFUSED,
                }))?;
                return Ok(REFUSED);
            }
        }
    }

    if dry_run {
        print_json(&json!({
            "dry_run": true,
            "project": args.project,
            "as": args.agent,
            "paths": paths,
            "reason": args.reason,
            "wait_secs": args.wait_secs,
            "ttl_secs": args.ttl_secs,
            "exclusive": true,
            "would_run": args.command,
            "note": "nothing was leased and nothing ran; re-run without --dry-run",
        }))?;
        return Ok(0);
    }

    let store = LeaseStore::resolve().await?;
    store.warn_if_ghost(&args.project, &args.agent).await;

    let mut held: BTreeSet<String> = BTreeSet::new();
    let mut blockers: std::collections::BTreeMap<String, Vec<String>> =
        std::collections::BTreeMap::new();
    let mut acquire_attempts: u64 = 0;
    let started = std::time::Instant::now();
    let deadline = started + Duration::from_secs(args.wait_secs);
    let refusal: Option<Refusal> = loop {
        let missing: Vec<String> = paths
            .iter()
            .filter(|p| !held.contains(*p))
            .cloned()
            .collect();
        if missing.is_empty() {
            break None;
        }
        acquire_attempts += 1;
        let acquired = store.acquire(args, &missing).await?;
        for path in granted_paths(&acquired) {
            held.insert(path);
        }
        if held.len() == paths.len() {
            break None;
        }
        let hard = blocked_paths(&acquired, "conflicts");
        let soft = blocked_paths(&acquired, "soft_conflicts");
        for (path, holders) in &hard {
            blockers.insert(path.clone(), holders.clone());
        }
        if !soft.is_empty() {
            break Some(Refusal {
                intent: true,
                blocks: soft,
            });
        }
        if std::time::Instant::now() >= deadline {
            break Some(Refusal {
                intent: false,
                blocks: blockers.clone().into_iter().collect(),
            });
        }
        // Never sleep while holding a partial grant: another agent may need
        // the paths we hold while we wait for the rest (hold-and-wait). Drop
        // them and retry the full set on the next poll.
        if !held.is_empty() {
            if let Err(e) = store.release(args).await {
                eprintln!(
                    "toron reserve with-lease: release of partial grants failed (they expire \
                     on their own): {e:#}"
                );
            }
            held.clear();
        }
        tokio::time::sleep(POLL).await;
    };
    if let Some(Refusal {
        intent: intent_blocked,
        blocks,
    }) = refusal
    {
        if let Err(e) = store.release(args).await {
            eprintln!("toron reserve with-lease: release after refusal failed: {e:#}");
        }
        let named: Vec<String> = blocks
            .iter()
            .map(|(path, holders)| {
                let what = if intent_blocked {
                    "has a declared intent"
                } else {
                    "is held exclusively"
                };
                if holders.is_empty() {
                    format!("`{path}` {what} by another pin")
                } else {
                    format!("`{path}` {what} by {}", holders.join(", "))
                }
            })
            .collect();
        let detail = if named.is_empty() {
            "the paths could not be granted".to_string()
        } else {
            named.join("; ")
        };
        let remedy = if intent_blocked {
            "an intent is advisory but freezes the path; the declarer must release or convert it \
             (`toron reserve request-release`, then re-run)"
        } else {
            "re-run with a longer --wait-secs, or release / request-release the hold"
        };
        eprintln!("toron reserve with-lease: refused — {detail}. Nothing was run. {remedy}.");
        print_json(&json!({
            "acquired": false,
            "refused": true,
            "project": args.project,
            "as": args.agent,
            "paths": args.paths,
            "reason": args.reason,
            "conflicts": blocks.iter().map(|(path, holders)| json!({
                "path": path,
                "holders": holders,
                "tier": if intent_blocked { "intent" } else { "held" },
            })).collect::<Vec<_>>(),
            "acquire_attempts": acquire_attempts,
            "exit_code": REFUSED,
        }))?;
        return Ok(REFUSED);
    }

    let mut signals = match Signals::install() {
        Ok(signals) => signals,
        Err(e) => {
            let _ = store.release(args).await;
            return Err(e.context(
                "cannot install the SIGINT/SIGTERM traps — refusing to run a command that \
                 could leave its lease behind",
            ));
        }
    };

    let ttl = args
        .ttl_secs
        .unwrap_or(toron_core::mailbox::reservations::DEFAULT_RESERVATION_TTL_SECS);
    let extend = (ttl / 2).max(5);
    let heartbeat = {
        let project = args.project.clone();
        let agent = args.agent.clone();
        let paths = args.paths.clone();
        let period = Duration::from_secs(extend);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(period).await;
                match LeaseStore::resolve().await {
                    Ok(store) => {
                        if let Err(e) = store.renew(&project, &agent, &paths, extend).await {
                            eprintln!(
                                "toron reserve with-lease: heartbeat renew failed (the lease \
                                 will expire within {ttl}s): {e:#}"
                            );
                        }
                    }
                    Err(e) => eprintln!(
                        "toron reserve with-lease: heartbeat cannot reach the mailbox: {e:#}"
                    ),
                }
            }
        })
    };

    let mut command = tokio::process::Command::new(&args.command[0]);
    command.args(&args.command[1..]);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            let release = store.release(args).await;
            eprintln!(
                "toron reserve with-lease: failed to spawn `{}`: {e}",
                args.command.join(" ")
            );
            print_json(&summary(
                args,
                127,
                None,
                release,
                started,
                acquire_attempts,
            ))?;
            return Ok(127);
        }
    };

    let (code, signal) = wait_child(&mut signals, &mut child).await;
    heartbeat.abort();

    let release = store.release(args).await;
    if let Err(e) = &release {
        eprintln!("toron reserve with-lease: RELEASE FAILED — the lease may still be held: {e:#}");
    }
    print_json(&summary(
        args,
        code,
        signal,
        release,
        started,
        acquire_attempts,
    ))?;
    Ok(code)
}

/// Why the wait loop gave up: the tier that blocked it (the kernel froze a
/// declared intent; or a peer's exclusive hold outlived `--wait-secs`) and
/// the unacquirable paths with their last-seen holders.
struct Refusal {
    intent: bool,
    blocks: Vec<(String, Vec<String>)>,
}

/// The wrapper's own result row (the child's output already went to the
/// inherited stdio; this is the one summary of what the wrapper did).
fn summary(
    args: &ReserveWithLeaseArgs,
    exit_code: u8,
    signal: Option<i32>,
    release: anyhow::Result<Value>,
    started: std::time::Instant,
    acquire_attempts: u64,
) -> Value {
    let (released, release_error) = match release {
        Ok(value) => (released_count(&value), Value::Null),
        Err(e) => (0, json!(format!("{e:#}"))),
    };
    json!({
        "project": args.project,
        "as": args.agent,
        "paths": args.paths,
        "reason": args.reason,
        "command": args.command,
        "exit_code": exit_code,
        "signal": signal,
        "released": released,
        "release_error": release_error,
        "acquire_attempts": acquire_attempts,
        "held_secs": started.elapsed().as_secs(),
    })
}

/// SIGINT/SIGTERM traps, installed before the child exists.
struct Signals {
    #[cfg(unix)]
    sigint: tokio::signal::unix::Signal,
    #[cfg(unix)]
    sigterm: tokio::signal::unix::Signal,
}

impl Signals {
    fn install() -> anyhow::Result<Self> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                sigint: signal(SignalKind::interrupt())?,
                sigterm: signal(SignalKind::terminate())?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }
}

/// Wait for the child; a trapped signal kills it (bounded grace first, in
/// case the terminal already delivered the same signal to the foreground
/// group) and reports 128+signum.
async fn wait_child(signals: &mut Signals, child: &mut tokio::process::Child) -> (u8, Option<i32>) {
    #[cfg(unix)]
    {
        tokio::select! {
            status = child.wait() => exit_code_of(status),
            _ = signals.sigint.recv() => die_with(child, 2).await,
            _ = signals.sigterm.recv() => die_with(child, 15).await,
        }
    }
    #[cfg(not(unix))]
    {
        let _ = signals;
        exit_code_of(child.wait().await)
    }
}

#[cfg(unix)]
async fn die_with(child: &mut tokio::process::Child, signum: u8) -> (u8, Option<i32>) {
    if tokio::time::timeout(SIGNAL_GRACE, child.wait())
        .await
        .is_err()
    {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    (128 + signum, Some(signum as i32))
}

fn exit_code_of(status: std::io::Result<std::process::ExitStatus>) -> (u8, Option<i32>) {
    match status {
        Ok(status) => {
            if let Some(code) = status.code() {
                (code.clamp(0, 255) as u8, None)
            } else {
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    let sig = status.signal().unwrap_or(0);
                    ((128 + sig).clamp(0, 255) as u8, Some(sig))
                }
                #[cfg(not(unix))]
                {
                    (1, None)
                }
            }
        }
        Err(_) => (1, None),
    }
}

/// The mailbox selector, exactly the robot path's: a daemon when one serves
/// (acquire/release are existing MCP tools), else the durable XDG root.
enum LeaseStore {
    Embedded(std::sync::Arc<Mailbox>),
    Daemon(crate::mailbox_source::DaemonClient),
}

impl LeaseStore {
    async fn resolve() -> anyhow::Result<Self> {
        Ok(match MailboxSource::resolve().await? {
            MailboxSource::Embedded(mb) => Self::Embedded(mb),
            MailboxSource::Daemon(dc) => Self::Daemon(dc),
        })
    }

    /// Ghost-holder nudge, same as `reserve acquire`: a lease held by an
    /// unregistered pin cannot be resolved by whois/guard. Advisory only,
    /// never blocks.
    async fn warn_if_ghost(&self, project: &str, agent: &str) {
        match self {
            Self::Embedded(mb) => crate::robot::warn_if_unregistered(mb, project, agent).await,
            Self::Daemon(_) => {
                if let Ok(mb) = toron_mcp::demo_mailbox().await {
                    crate::robot::warn_if_unregistered(&mb, project, agent).await;
                }
            }
        }
    }

    /// Exclusive by construction: two scoped commands on one path must never
    /// overlap, so `exclusive: true` regardless of what `acquire` would do.
    async fn acquire(
        &self,
        args: &ReserveWithLeaseArgs,
        paths: &[String],
    ) -> anyhow::Result<Value> {
        match self {
            Self::Embedded(mb) => Ok(mb
                .file_reservation_paths(
                    &args.project,
                    &args.agent,
                    paths,
                    args.ttl_secs,
                    true,
                    Some(&args.reason),
                )
                .await?
                .to_json()),
            Self::Daemon(dc) => {
                dc.call_tool(
                    "file_reservation_paths",
                    json!({
                        "project_key": args.project,
                        "agent_name": args.agent,
                        "paths": paths,
                        "ttl_seconds": args.ttl_secs,
                        "exclusive": true,
                        "reason": args.reason,
                    }),
                )
                .await
            }
        }
    }

    /// Extend this agent's leases. The heartbeat path: a scoped command can
    /// outlive the TTL, so the wrapper renews every half-TTL while it runs.
    async fn renew(
        &self,
        project: &str,
        agent: &str,
        paths: &[String],
        extend_secs: u64,
    ) -> anyhow::Result<Value> {
        match self {
            Self::Embedded(mb) => Ok(mb
                .renew_file_reservations(project, agent, paths, extend_secs)
                .await?
                .to_json()),
            Self::Daemon(dc) => {
                dc.call_tool(
                    "renew_file_reservations",
                    json!({
                        "project_key": project,
                        "agent_name": agent,
                        "paths": paths,
                        "extend_seconds": extend_secs,
                    }),
                )
                .await
            }
        }
    }

    /// Release this agent's leases on the requested paths (idempotent; only
    /// this agent's holds are touched — a peer's lease always survives).
    async fn release(&self, args: &ReserveWithLeaseArgs) -> anyhow::Result<Value> {
        match self {
            Self::Embedded(mb) => Ok(mb
                .release_file_reservations(&args.project, &args.agent, &args.paths)
                .await?
                .to_json()),
            Self::Daemon(dc) => {
                dc.call_tool(
                    "release_file_reservations",
                    json!({
                        "project_key": args.project,
                        "agent_name": args.agent,
                        "paths": args.paths,
                    }),
                )
                .await
            }
        }
    }
}

fn granted_paths(acquire: &Value) -> Vec<String> {
    acquire
        .get("granted")
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    r.get("path_pattern")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// `(path, holders)` rows from the acquire result's `conflicts` (hard, held)
/// or `soft_conflicts` (intent) array.
fn blocked_paths(acquire: &Value, key: &str) -> Vec<(String, Vec<String>)> {
    acquire
        .get(key)
        .and_then(Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(|r| {
                    let path = r.get("path")?.as_str()?.to_string();
                    let holders = r
                        .get("holders")
                        .and_then(Value::as_array)
                        .map(|hs| {
                            hs.iter()
                                .filter_map(|h| h.as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    Some((path, holders))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn released_count(release: &Value) -> u64 {
    release.get("released").and_then(Value::as_u64).unwrap_or(0)
}

/// A `--paths` glob that matches zero files would acquire a lease over
/// nothing while reporting success — the exact phantom this wrapper exists to
/// prevent, so it refuses (unlike `acquire`, which only warns). A bare
/// directory gets the both-level reminder: the measured way a flip lease
/// covered no file.
fn zero_match_refusal(pattern: &str, files: &[String], is_dir: bool) -> Option<String> {
    if guard::count_pattern_matches(pattern, files) > 0 {
        return None;
    }
    Some(if is_dir {
        let dir = pattern.trim_end_matches('/');
        format!(
            "`{pattern}` is a directory, not a file glob — it matches 0 files. Reserve BOTH \
             levels (`{dir}/*` + `{dir}/**/*`) when you mean the whole tree. Nothing was \
             leased and the command did not run."
        )
    } else {
        format!(
            "glob `{pattern}` matched 0 files under the repo. fnmatch rule: `**` collapses to \
             `*` but a literal `/` still needs a `/` in the path, so `src/**/*.rs` misses \
             top-level `src/*.rs`. Reserve BOTH levels (`src/*.rs` + `src/**/*.rs`) when you \
             mean the whole tree. Nothing was leased and the command did not run."
        )
    })
}

/// A bare directory (no glob metacharacters) that exists in the cwd.
fn is_bare_dir(pattern: &str) -> bool {
    !pattern.contains(['*', '?', '[']) && std::path::Path::new(pattern).is_dir()
}

fn print_json(value: &Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn files() -> Vec<String> {
        vec![
            "Cargo.toml".into(),
            "src/main.rs".into(),
            "src/nested/lib.rs".into(),
        ]
    }

    /// The refusal must be fail-closed AND actionable: the measured miss
    /// (`src/**/*.rs` never matches top-level files) names the both-level
    /// form, and a bare directory says so instead of echoing fnmatch trivia.
    #[test]
    fn zero_match_refusal_is_fail_closed_and_names_the_both_level_fix() {
        assert!(
            zero_match_refusal("src/main.rs", &files(), false).is_none(),
            "a matching glob must not refuse"
        );
        let refusal = zero_match_refusal("src/**/*.md", &files(), false).expect("must refuse");
        assert!(refusal.contains("0 files"), "{refusal}");
        assert!(
            refusal.contains("src/*.rs") && refusal.contains("src/**/*.rs"),
            "the remedy must name both levels: {refusal}"
        );
        assert!(refusal.contains("did not run"), "{refusal}");

        let dir = zero_match_refusal("src", &files(), true).expect("must refuse a bare dir");
        assert!(dir.contains("directory"), "{dir}");
        assert!(dir.contains("src/*") && dir.contains("src/**/*"), "{dir}");
    }

    #[test]
    fn bare_dir_detection_ignores_globs() {
        assert!(is_bare_dir("src"), "src/ exists under the crate root");
        assert!(!is_bare_dir("src/*"), "a glob is not a bare dir");
        assert!(!is_bare_dir("zz-no-such-dir"), "a missing dir is not a dir");
    }

    /// The wait loop's classification reads the acquire parity JSON
    /// (`granted` / `conflicts` / `soft_conflicts`), never a typed struct —
    /// the same shape arrives from the daemon arm.
    #[test]
    fn acquire_result_parsing_reads_granted_and_blocked_rows() {
        let value = json!({
            "granted": [
                {"id": "e1", "path_pattern": "a.rs", "exclusive": true, "reason": "r", "expires_ts_us": null}
            ],
            "conflicts": [
                {"path": "b.rs", "holders": ["OtherPin"], "reservation_ids": ["e2"]}
            ],
            "soft_conflicts": [
                {"path": "c.rs", "holders": ["Planner"], "reservation_ids": []}
            ],
        });
        assert_eq!(granted_paths(&value), vec!["a.rs"]);
        assert_eq!(
            blocked_paths(&value, "conflicts"),
            vec![("b.rs".to_string(), vec!["OtherPin".to_string()])]
        );
        assert_eq!(blocked_paths(&value, "soft_conflicts")[0].0, "c.rs");
        assert!(granted_paths(&json!({})).is_empty());
        assert!(blocked_paths(&json!({}), "conflicts").is_empty());
        assert_eq!(released_count(&json!({"released": 2})), 2);
    }

    #[test]
    fn unix_exit_codes_pass_through_and_signals_read_128_plus_n() {
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            let status = std::process::ExitStatus::from_raw(7 << 8);
            assert_eq!(exit_code_of(Ok(status)), (7, None));
            let killed = std::process::ExitStatus::from_raw(15);
            assert_eq!(exit_code_of(Ok(killed)), (143, Some(15)));
        }
        let _ = exit_code_of(Err(std::io::Error::other("no status")));
    }
}
