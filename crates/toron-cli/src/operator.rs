// governed-by: ADR-0003
// governed-by: ADR-0004 (run_doctor_reconstruct — Ship 3)

//! Operator surface (bd-led.8.2): doctor/metrics/key/project/relay/backend.
//!
//! The daemon-persona commands. Read ops emit JSON on stdout (clig.dev:
//! stdout = data, stderr = messages). Exit-code discipline: 0 ok · 1 runtime ·
//! 2 usage (clap) · 3 doctor `--check` fails-closed.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde_json::json;

use crate::cli::{
    AgentCommand, BackendCommand, Command, ContactCommand, DoctorArgs, DoctorCmd,
    DoctorReconstructArgs, GuardCommand, KeyCommand, KeyGenerateArgs, KeyInspectArgs, MailCommand,
    ProjectCommand, ProjectEnsureArgs, RelayCommand, RelayUrlArg, ReserveCommand, SlotCommand,
};
use crate::config::Config;
use crate::output::{OutputFormat, render_json};
use toron_mcp::ToJson;

/// scrypt log-n for operator keys (P3 bd-p3-argon2-kdf-dos-bound-7sbu:
/// raised 12 -> 14 — matches toron-core OPERATOR_KEY_LOG_N; 12 was 16x
/// weaker than agent keys (NIP-49 log_n=16). Decrypt reads log_n from the
/// payload, so existing keys keep working).
const KEY_LOG_N: u8 = 14;

/// Dev-default passphrase when `TORON_KEY_PASSPHRASE` is unset. Production
/// MUST set the env var; the flag form is intentionally rejected.
fn key_password() -> String {
    std::env::var("TORON_KEY_PASSPHRASE").unwrap_or_else(|_| "toron".to_string())
}

/// Print a JSON value to stdout as pretty JSON.
fn print_json(value: &serde_json::Value) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

#[derive(serde::Serialize)]
struct CheckResult {
    name: &'static str,
    ok: bool,
    detail: String,
}

/// bd-legacy-slug-inbox-onoj: list slug identities whose registry fold
/// points elsewhere — standalone registrations a fold never absorbed (a
/// second identity beside the pin; mail sealed to its npub is unreachable).
/// Informational (ok stays true): doctor NAMES the rows, cleanup is the
/// fold path's job (`fold_agent_name_alias` absorbs idempotently).
/// Source time for the live-daemon judgment (wave9-c): HEAD commit time in
/// this checkout, else the newest source mtime under a `Cargo.toml` root
/// (exported trees have no git), else `None` = undiscoverable. The walk
/// skips `target/`, `.git/`, `node_modules/` and caps at 20k entries — a
/// doctor probe, not an inventory.
fn discover_source_time() -> Option<i64> {
    if let Some(ct) = head_commit_time() {
        return Some(ct);
    }
    let root = std::env::current_dir().ok()?;
    if !root.join("Cargo.toml").is_file() {
        return None;
    }
    let mut newest: Option<i64> = None;
    let mut stack = vec![root];
    let mut seen = 0usize;
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            seen += 1;
            if seen > 20_000 {
                return newest;
            }
            let path = entry.path();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if !matches!(name, "target" | ".git" | "node_modules") {
                    stack.push(path);
                }
                continue;
            }
            let mtime = std::fs::metadata(&path)
                .ok()
                .and_then(|md| md.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64);
            newest = newest.max(mtime);
        }
    }
    newest
}

/// HEAD commit unix time in the current checkout, else `None`.
fn head_commit_time() -> Option<i64> {
    let out = std::process::Command::new("git")
        .args(["log", "-1", "--format=%ct"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout)
        .ok()?
        .trim()
        .parse::<i64>()
        .ok()
}

/// Live-daemon leg (wave9-c): judge the RUNNING daemon's build against the
/// source, not the local target. No serving daemon (or no boot record) keeps
/// the existing not-running result — never a false failure. A stale daemon
/// fails with the one remedy: reinstall toron from this source, then
/// `toron daemon restart`. Never revives anything.
fn check_daemon_freshness() -> CheckResult {
    use toron_core::serve_build::{DaemonFreshness, daemon_freshness_verdict};
    let name = "daemon_freshness";
    let record_path = crate::daemon::build_record_path();
    let Some(snap) = toron_core::serve_build::read(&record_path) else {
        return CheckResult {
            name,
            ok: true,
            detail: "no boot record — no toron serve has booted against this data root".into(),
        };
    };
    let serving = crate::daemon::port_holder_pid(crate::daemon::DEFAULT_ADDR);
    let on_disk = toron_core::serve_build::artifact_id(&snap.binary);
    let source = discover_source_time();
    let daemon_ts = crate::daemon::ts(snap.built_at_unix);
    match daemon_freshness_verdict(serving, &snap, on_disk, source) {
        DaemonFreshness::Fresh => CheckResult {
            name,
            ok: true,
            detail: format!(
                "daemon pid {} runs {} (build written {}) vs source {} — current",
                snap.pid,
                snap.binary.display(),
                daemon_ts,
                source
                    .map(crate::daemon::ts)
                    .as_deref()
                    .unwrap_or("<undiscoverable>")
            ),
        },
        DaemonFreshness::NotRunning => CheckResult {
            name,
            ok: true,
            detail: format!(
                "build record is from pid {} — no live toron-managed daemon serves now",
                snap.pid
            ),
        },
        DaemonFreshness::Stale => {
            let cause = if on_disk != Some(snap.artifact()) {
                "the file on disk is a different build (redeployed under it)".to_string()
            } else {
                format!(
                    "older than the source {}",
                    source
                        .map(crate::daemon::ts)
                        .as_deref()
                        .unwrap_or("<undiscoverable>")
                )
            };
            CheckResult {
                name,
                ok: false,
                detail: format!(
                    "daemon pid {} runs {} (build written {}) {cause} — \
                     remedy: reinstall toron from this source, then `toron daemon restart`",
                    snap.pid,
                    snap.binary.display(),
                    daemon_ts,
                ),
            }
        }
    }
}

/// Source HEAD for the local-binary leg (wave7-c), when discoverable: the
/// checkout the operator invoked from. `None` anywhere else (installed binary
/// run outside a checkout) — the triple names the absence instead of guessing.
fn discover_head() -> Option<String> {
    let out = std::process::Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let sha = String::from_utf8(out.stdout).ok()?;
    let sha = sha.trim().to_string();
    (!sha.is_empty()).then_some(sha)
}

/// Local-binary leg (wave7-c, relabeled wave9-c): source HEAD (if
/// discoverable), installed binary (path + version + mtime), store schema
/// (applied vs what this binary resolves). This judges the TARGET on disk —
/// the live daemon has its own leg (`daemon_freshness`). A store newer than
/// the binary FAILS with the remedy (upgrade toron / open with the newer
/// binary); a green triple carries no remedy — it is silent. Binary-vs-HEAD
/// staleness is NOT judged here:
/// mtimes lie (see `serve_build` docs) — the pair is named so the operator
/// judges, and a live stale daemon stays the `daemon_build` check's job.
fn check_freshness(cfg: &Config) -> CheckResult {
    let name = "local_binary";
    let binary = std::env::current_exe()
        .ok()
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| "<unknown>".into());
    let version = env!("CARGO_PKG_VERSION");
    let head = discover_head().unwrap_or_else(|| "<undiscoverable — not in a checkout>".into());
    let triple = format!("head={head} binary={binary}@{version}");

    let Some(dir) = resolve_data_dir(cfg, None) else {
        return CheckResult {
            name,
            ok: true,
            detail: format!("{triple} store=<no data dir yet>"),
        };
    };
    let db = dir.join("index.sqlite");
    if !db.exists() {
        return CheckResult {
            name,
            ok: true,
            detail: format!(
                "{triple} store={} (no index yet — created on first run)",
                db.display()
            ),
        };
    }
    let known = toron_core::index::schema::resolved_max_version();
    let applied = (|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .ok()?;
        rt.block_on(async {
            let pool = toron_core::index::schema::connect(db.to_str()?)
                .await
                .ok()?;
            toron_core::index::schema::store_version(&pool).await.ok()
        })
    })();
    let Some(applied) = applied else {
        return CheckResult {
            name,
            ok: false,
            detail: format!(
                "{triple} store={} unreadable — remedy: check the file, restore from backup, or delete it for a fresh store",
                db.display()
            ),
        };
    };
    if applied.min_binary > known {
        return CheckResult {
            name,
            ok: false,
            detail: format!(
                "{triple} store={} schema applied={} newer than this binary resolves={} — remedy: upgrade toron, or open the store with the newer binary (downgrade needs a rebuild)",
                db.display(),
                applied.min_binary,
                known
            ),
        };
    }
    CheckResult {
        name,
        ok: true,
        detail: format!(
            "{triple} store={} schema applied={} resolved={}",
            db.display(),
            applied.min_binary,
            known
        ),
    }
}

fn check_engine_store(cfg: &Config) -> CheckResult {
    let Some(dir) = resolve_data_dir(cfg, None) else {
        return CheckResult {
            name: "engine_store",
            ok: true,
            detail: "no data dir yet — the store is created on first run".into(),
        };
    };
    let db = dir.join("index.sqlite");
    if !db.exists() {
        return CheckResult {
            name: "engine_store",
            ok: true,
            detail: format!("no index yet at {} — created on first run", db.display()),
        };
    }
    let writable = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&db)
        .is_ok();
    CheckResult {
        name: "engine_store",
        ok: writable,
        detail: format!(
            "in-process durable engine (ADR-0043): {} {}",
            db.display(),
            if writable { "writable" } else { "NOT writable" }
        ),
    }
}

fn check_legacy_slug_identities(cfg: &Config) -> CheckResult {
    let name = "legacy_slug_identities";
    let Some(data_dir) = resolve_data_dir(cfg, None) else {
        return CheckResult {
            name,
            ok: true,
            detail: "no data dir — nothing to scan".into(),
        };
    };
    let db_path = data_dir.join("index.sqlite");
    if !db_path.exists() {
        return CheckResult {
            name,
            ok: true,
            detail: format!("no index at {}", db_path.display()),
        };
    }
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            return CheckResult {
                name,
                ok: false,
                detail: format!("runtime build failed: {e}"),
            };
        }
    };
    let scanned = rt.block_on(async {
        let pool = toron_core::index::schema::connect(db_path.to_str().unwrap()).await?;
        let leftovers = toron_core::mailbox::identity::folded_slug_leftovers(&pool).await?;
        pool.close().await;
        Ok::<_, anyhow::Error>(leftovers)
    });
    match scanned {
        Ok(leftovers) if leftovers.is_empty() => CheckResult {
            name,
            ok: true,
            detail: "no folded-slug dual registrations".into(),
        },
        Ok(leftovers) => {
            let rows: Vec<String> = leftovers
                .iter()
                .map(|l| {
                    format!(
                        "{}:{} → {} (npub {})",
                        l.project_slug, l.slug, l.canonical, l.pubkey
                    )
                })
                .collect();
            CheckResult {
                name,
                ok: true,
                detail: format!(
                    "{} leftover legacy registration(s): {}",
                    leftovers.len(),
                    rows.join("; ")
                ),
            }
        }
        Err(e) => CheckResult {
            name,
            ok: false,
            detail: format!("scan failed: {e:#}"),
        },
    }
}

/// bd-0tbg W1: identities in ONE project that fold to the same words and so
/// hold separate keys — `BrashOtter` and `brash-otter` measured live, 4 and 5
/// reservations between them. A release or a conflict check addressed to one
/// spelling cannot see the other's leases.
///
/// `ok` is FALSE here, unlike the informational slug check beside it: this is
/// not leftover bookkeeping, it is two identities where the operator believes
/// there is one, and every reservation under the shadowed spelling is
/// unaddressable by the name a human would use. The reconciliation is a human
/// decision (bd-lkjf) — which key survives cannot be inferred from the board.
fn check_name_collisions(cfg: &Config) -> CheckResult {
    let name = "name_collisions";
    let Some(data_dir) = resolve_data_dir(cfg, None) else {
        return CheckResult {
            name,
            ok: true,
            detail: "no data dir — nothing to scan".into(),
        };
    };
    let db_path = data_dir.join("index.sqlite");
    if !db_path.exists() {
        return CheckResult {
            name,
            ok: true,
            detail: format!("no index at {}", db_path.display()),
        };
    }
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            return CheckResult {
                name,
                ok: false,
                detail: format!("runtime build failed: {e}"),
            };
        }
    };
    let scanned = rt.block_on(async {
        let pool = toron_core::index::schema::connect(db_path.to_str().unwrap()).await?;
        let found = toron_core::mailbox::identity::name_collisions_across_projects(&pool).await?;
        pool.close().await;
        Ok::<_, anyhow::Error>(found)
    });
    match scanned {
        Ok(found) if found.is_empty() => CheckResult {
            name,
            ok: true,
            detail: "no identity holds a second key for the same name".into(),
        },
        Ok(found) => {
            let rows: Vec<String> = found
                .iter()
                .map(|(p, g)| {
                    let keys: Vec<String> = g
                        .pubkeys
                        .iter()
                        .map(|k| k[..8.min(k.len())].to_string())
                        .collect();
                    format!(
                        "{}: [{}] -> {} key(s) {}",
                        p,
                        g.names.join(", "),
                        keys.len(),
                        keys.join("/")
                    )
                })
                .collect();
            CheckResult {
                name,
                ok: false,
                detail: format!(
                    "{} project(s) hold one name across several keys: {}. Registrations that \
                     would split a name are now refused, but these predate the check — \
                     reconciling them is a human decision (bd-lkjf), since only you know \
                     whether the spellings are one agent or two.",
                    found.len(),
                    rows.join("; ")
                ),
            }
        }
        Err(e) => CheckResult {
            name,
            ok: false,
            detail: format!("scan failed: {e:#}"),
        },
    }
}

/// bd-u31k: pure core of the identity-divergence check — each live pane's
/// stamped npub vs the registry's binding for that pin. Inputs are exactly
/// what the shell gathers: pane stamps and `agents` rows (project, name,
/// pubkey). A stamp whose (normalized project, pin) is not registered is NOT
/// a divergence (that is `whois`'s "register first"); a stamp whose npub is
/// not among the registry's keys for that name is the measured hazard —
/// mail sent to the name reaches a key the pane does not read, invisibly.
fn identity_divergence_check(
    stamps: &[toron_core::mailbox::identity::PaneStamp],
    roster: &[(String, String, String)],
) -> CheckResult {
    let name = "identity_divergence";
    let short = |k: &str| k[..8.min(k.len())].to_string();
    let mut rows: Vec<String> = Vec::new();
    for stamp in stamps {
        let slug = toron_core::mailbox::identity::stamp_project_slug(&stamp.project);
        let registered: Vec<&str> = roster
            .iter()
            .filter(|(p, n, _)| p == slug && n == &stamp.pin)
            .map(|(_, _, k)| k.as_str())
            .collect();
        if registered.is_empty() || registered.iter().any(|k| *k == stamp.npub) {
            continue;
        }
        let keys: Vec<String> = registered.iter().map(|k| short(k)).collect();
        rows.push(format!(
            "pane {} stamps {} ({slug}) npub {}… but the registry binds it to {}… — \
             re-register the pin or address it by npub",
            stamp.pane_id,
            stamp.pin,
            short(&stamp.npub),
            keys.join("/")
        ));
    }
    if rows.is_empty() {
        CheckResult {
            name,
            ok: true,
            detail: format!(
                "{} stamped pane(s) — every stamp npub matches its registered pin",
                stamps.len()
            ),
        }
    } else {
        CheckResult {
            name,
            ok: false,
            detail: format!("{} divergent pin(s): {}", rows.len(), rows.join("; ")),
        }
    }
}

/// bd-u31k: the shell around [`identity_divergence_check`] — stamped panes
/// from hc, registry rows from the index DB. hc unreachable or no
/// index is NOT a defect (the same advisory posture as the pane fields
/// `whois` attaches), so those read ok with the reason named.
fn check_identity_divergence(cfg: &Config) -> CheckResult {
    let name = "identity_divergence";
    let stamps = {
        let Ok(backend) = toron_plane::backend::plane::connect_default() else {
            return CheckResult {
                name,
                ok: true,
                detail: "pane plane not reachable — no live panes to compare".into(),
            };
        };
        let panes: Vec<toron_plane::backend::PaneInfo> =
            backend.pane_list(None).unwrap_or_default();
        panes
            .iter()
            .filter_map(toron_core::mailbox::identity::pane_stamp)
            .collect::<Vec<_>>()
    };
    if stamps.is_empty() {
        return CheckResult {
            name,
            ok: true,
            detail: "no stamped live panes to compare".into(),
        };
    }
    let Some(data_dir) = resolve_data_dir(cfg, None) else {
        return CheckResult {
            name,
            ok: true,
            detail: "no data dir — nothing to compare".into(),
        };
    };
    let db_path = data_dir.join("index.sqlite");
    if !db_path.exists() {
        return CheckResult {
            name,
            ok: true,
            detail: format!("no index at {}", db_path.display()),
        };
    }
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            return CheckResult {
                name,
                ok: false,
                detail: format!("runtime build failed: {e}"),
            };
        }
    };
    let roster = rt.block_on(async {
        let pool = toron_core::index::schema::connect(db_path.to_str().unwrap()).await?;
        let rows = toron_core::mailbox::identity::identity_roster(&pool).await?;
        pool.close().await;
        Ok::<_, anyhow::Error>(rows)
    });
    match roster {
        Ok(roster) => identity_divergence_check(&stamps, &roster),
        Err(e) => CheckResult {
            name,
            ok: false,
            detail: format!("scan failed: {e:#}"),
        },
    }
}

/// The daemon can be live and still not be the build on disk — a process keeps
/// executing the file it was exec'd from after a rebuild replaces that file.
/// That cost a full debug cycle on 2026-09-30: an hour-stale daemon rewrote the
/// operator key on every boot while every other check read green, and nothing
/// on the machine said which build was serving.
///
/// FAILS rather than advising: a daemon serving a build you did not build is a
/// real inconsistency, and the remedy is one command. A record that names an
/// EARLIER boot (nothing serving now, or another process holds the address) is
/// not stale — it is just not judgeable, and is reported as ok.
fn check_daemon_build() -> CheckResult {
    let name = "daemon_build";
    let Some(snap) = toron_core::serve_build::read(&crate::daemon::build_record_path()) else {
        return CheckResult {
            name,
            ok: true,
            detail: "no boot record — no toron serve has booted against this data root".into(),
        };
    };
    let serving = crate::daemon::port_holder_pid(crate::daemon::DEFAULT_ADDR);
    let verdict = toron_core::serve_build::build_verdict(
        &snap,
        serving,
        toron_core::serve_build::artifact_id(&snap.binary),
    );
    CheckResult {
        name,
        ok: !verdict.is_replaced(),
        detail: crate::daemon::build_detail(
            &snap,
            verdict,
            serving,
            toron_core::serve_build::artifact_mtime_unix(&snap.binary),
        ),
    }
}

fn check_hc() -> CheckResult {
    let path = toron_plane::backend::hc::resolve_socket_path(
        None,
        std::env::var("HC_SOCKET_PATH").ok().as_deref(),
        std::env::var_os("XDG_STATE_HOME")
            .map(std::path::PathBuf::from)
            .as_deref(),
        std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .as_deref(),
        std::env::var("HC_SESSION").ok().as_deref(),
    );
    match toron_plane::backend::hc::HcBackend::connect(None) {
        Ok(_) => CheckResult {
            name: "hc",
            ok: true,
            detail: format!("hc socket reachable at {}", path.display()),
        },
        Err(e) => CheckResult {
            name: "hc",
            ok: false,
            detail: format!("hc not present ({}): {e}", path.display()),
        },
    }
}

fn check_git() -> CheckResult {
    match std::process::Command::new("git").arg("--version").output() {
        Ok(out) if out.status.success() => CheckResult {
            name: "git",
            ok: true,
            detail: String::from_utf8_lossy(&out.stdout).trim().to_string(),
        },
        Ok(_) => CheckResult {
            name: "git",
            ok: false,
            detail: "git present but `git --version` failed".to_string(),
        },
        Err(e) => CheckResult {
            name: "git",
            ok: false,
            detail: format!("git not found: {e}"),
        },
    }
}

fn check_keys(cfg: &Config) -> CheckResult {
    match &cfg.operator_key {
        Some(p) if p.is_file() => CheckResult {
            name: "keys",
            ok: true,
            detail: format!("operator key present at {} (configured)", p.display()),
        },
        Some(p) => CheckResult {
            name: "keys",
            ok: false,
            detail: format!("operator key missing at {} (configured)", p.display()),
        },
        None => {
            let canonical = toron_core::config::operator_key_path();
            if canonical.is_file() {
                return CheckResult {
                    name: "keys",
                    ok: true,
                    detail: format!(
                        "operator key present at {} (canonical default)",
                        canonical.display()
                    ),
                };
            }
            let legacy = toron_core::config::legacy_operator_key_path();
            if legacy.is_file() {
                return CheckResult {
                    name: "keys",
                    ok: false,
                    detail: format!(
                        "no operator key at {canonical}, but a LEGACY key from the old data root \
                         sits at {legacy} — different identities, so nothing is chosen \
                         automatically; move the one you mean to keep (ADR-0038): \
                         mv {legacy} {canonical}",
                        canonical = canonical.display(),
                        legacy = legacy.display()
                    ),
                };
            }
            CheckResult {
                name: "keys",
                ok: false,
                detail: "operator_key not configured (set operator_key or TORON_OPERATOR_KEY)"
                    .to_string(),
            }
        }
    }
}

/// Deep health checks. `--check` exits 3 when any check fails (fails-closed,
/// CI-friendly); 0 when all pass. Runtime failures still exit 1.
pub fn run_doctor(
    cfg: &Config,
    args: &DoctorArgs,
    format: OutputFormat,
) -> anyhow::Result<ExitCode> {
    if let Some(DoctorCmd::Reconstruct(rargs)) = &args.command {
        return run_doctor_reconstruct(cfg, rargs, format);
    }
    let checks = vec![
        check_hc(),
        check_git(),
        check_keys(cfg),
        check_daemon_build(),
        check_daemon_freshness(),
        check_freshness(cfg),
        check_engine_store(cfg),
        check_legacy_slug_identities(cfg),
        check_name_collisions(cfg),
        check_identity_divergence(cfg),
    ];
    let value = json!({ "checks": serde_json::to_value(&checks)? });

    match format {
        OutputFormat::Json | OutputFormat::Plain => render_json(&value, format),
        OutputFormat::Human => {
            println!("toron doctor");
            for c in &checks {
                let status = if c.ok { "ok  " } else { "FAIL" };
                println!("  {status} {} — {}", c.name, c.detail);
            }
        }
    }

    if args.check && checks.iter().any(|c| !c.ok) {
        Ok(ExitCode::from(3))
    } else {
        Ok(ExitCode::SUCCESS)
    }
}

/// One project's reconstruct outcome.
#[derive(Debug, Clone, serde::Serialize)]
struct ReconstructReport {
    slug: String,
    archive_messages: usize,
    index_rows_before: Option<i64>,
    rebuilt_rows: Option<usize>,
    /// Path to the forensic bundle (original db + manifest) when a rebuild
    /// ran. `None` for healthy/dry-run-skip.
    quarantine: Option<String>,
    /// "healthy" | "rebuilt" | "would_rebuild" | "failed".
    status: String,
    reason: Option<String>,
}

/// Resolve the data directory: `--data-dir` > `TORON_DATA_DIR` > config
/// `data_dir` > the durable XDG root (`$XDG_DATA_HOME/toron` /
/// `~/.local/share/toron`, ADR-0007 D3). Never a temp per-pid root — the
/// default is always Some.
fn resolve_data_dir(cfg: &Config, override_dir: Option<&Path>) -> Option<PathBuf> {
    // Single interpretation: the emptiness rule lives in `parse_env_path`
    // (infallible for set values — any non-empty string is a path), so an
    // empty `TORON_DATA_DIR` falls through here exactly as it does in
    // `Config::load`.
    let env_pin = toron_core::config::parse_env_path("TORON_DATA_DIR").unwrap_or(None);
    override_dir
        .map(PathBuf::from)
        .or(env_pin)
        .or_else(|| cfg.data_dir.clone())
        .or_else(|| Some(toron_core::config::durable_data_dir()))
}

/// Enumerate project roots under `data_dir`: subdirs that contain a
/// `messages/` archive (the Git archive signature). If `filter` is set,
/// restrict to that slug.
fn enumerate_projects(data_dir: &Path, filter: Option<&str>) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let Ok(entries) = std::fs::read_dir(data_dir) else {
        return out;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let slug = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        if let Some(f) = filter
            && f != slug
        {
            continue;
        }
        if path.join("messages").is_dir() {
            out.push(path);
        }
    }
    out
}

/// Count archived messages (`messages/**/*.json`) — mirrors the rebuild walk
/// so the pre-check count is directly comparable.
fn count_archive_messages(archive_root: &Path) -> usize {
    let messages_root = archive_root.join("messages");
    let mut count = 0;
    let Ok(years) = std::fs::read_dir(&messages_root) else {
        return 0;
    };
    for year in years.flatten() {
        if !year.path().is_dir() {
            continue;
        }
        let Ok(months) = std::fs::read_dir(year.path()) else {
            continue;
        };
        for month in months.flatten() {
            if !month.path().is_dir() {
                continue;
            }
            let Ok(files) = std::fs::read_dir(month.path()) else {
                continue;
            };
            for f in files.flatten() {
                if f.path().extension().map(|e| e == "json") == Some(true) {
                    count += 1;
                }
            }
        }
    }
    count
}

/// Inspect an existing index via the core helper (no raw sqlx in the CLI).
async fn inspect_index(db_path: &Path) -> anyhow::Result<(bool, bool, Option<i64>)> {
    toron_core::index::rebuild::inspect_index(db_path).await
}

/// Seconds since UNIX epoch as a decimal string (quarantine suffix).
fn unix_ts_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Reconstruct one project root (async inner; wrapped in block_on by the
/// sync entry point).
async fn reconstruct_project(root: &Path, dry_run: bool) -> anyhow::Result<ReconstructReport> {
    let slug = root
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let db_path = root.join("index.sqlite");
    let archive_messages = count_archive_messages(root);

    let (db_exists, integrity_ok, index_rows) = inspect_index(&db_path).await?;

    if db_exists && integrity_ok && index_rows == Some(archive_messages as i64) {
        return Ok(ReconstructReport {
            slug,
            archive_messages,
            index_rows_before: index_rows,
            rebuilt_rows: None,
            quarantine: None,
            status: "healthy".into(),
            reason: None,
        });
    }

    if dry_run {
        return Ok(ReconstructReport {
            slug,
            archive_messages,
            index_rows_before: index_rows,
            rebuilt_rows: None,
            quarantine: None,
            status: "would_rebuild".into(),
            reason: Some(reason_needs_rebuild(
                db_exists,
                integrity_ok,
                index_rows,
                archive_messages,
            )),
        });
    }

    let ts = unix_ts_secs();
    let forensic_dir = root.join(format!("doctor-forensic-{ts}"));
    let quarantine = if db_exists {
        std::fs::create_dir_all(&forensic_dir)?;
        for suffix in ["", "-wal", "-shm"] {
            let src = root.join(format!("index.sqlite{suffix}"));
            if src.exists() {
                let dest = forensic_dir.join(format!("index.sqlite{suffix}.orig"));
                std::fs::rename(&src, &dest)?;
            }
        }
        let manifest = json!({
            "slug": slug,
            "ts": ts,
            "archive_messages": archive_messages,
            "index_rows_before": index_rows,
            "action": "reconstruct",
        });
        std::fs::write(
            forensic_dir.join("manifest.json"),
            serde_json::to_string_pretty(&manifest)?,
        )?;
        Some(forensic_dir.to_string_lossy().into_owned())
    } else {
        None
    };

    let new_db = root.join("index.sqlite.new");
    let _ = std::fs::remove_file(&new_db);
    let new_db_str = new_db.to_string_lossy().to_string();
    let (rebuilt, rows) =
        match toron_core::index::rebuild::build_fresh_index(&new_db_str, root).await {
            Ok(pair) => pair,
            Err(e) => {
                let _ = std::fs::remove_file(&new_db);
                return Ok(ReconstructReport {
                    slug,
                    archive_messages,
                    index_rows_before: index_rows,
                    rebuilt_rows: None,
                    quarantine,
                    status: "failed".into(),
                    reason: Some(format!("rebuild failed: {e}")),
                });
            }
        };

    if rows != rebuilt || rebuilt != archive_messages {
        let _ = std::fs::remove_file(&new_db);
        return Ok(ReconstructReport {
            slug,
            archive_messages,
            index_rows_before: index_rows,
            rebuilt_rows: Some(rebuilt),
            quarantine,
            status: "failed".into(),
            reason: Some(format!(
                "verify mismatch: archive={archive_messages} rebuilt={rebuilt} rows={rows}"
            )),
        });
    }

    let _ = std::fs::remove_file(root.join("index.sqlite.new-wal"));
    let _ = std::fs::remove_file(root.join("index.sqlite.new-shm"));
    std::fs::rename(&new_db, &db_path)?;
    Ok(ReconstructReport {
        slug,
        archive_messages,
        index_rows_before: index_rows,
        rebuilt_rows: Some(rebuilt),
        quarantine,
        status: "rebuilt".into(),
        reason: None,
    })
}

/// Human-readable reason for why a rebuild is needed (dry-run explanation).
fn reason_needs_rebuild(
    db_exists: bool,
    integrity_ok: bool,
    index_rows: Option<i64>,
    archive_messages: usize,
) -> String {
    if !db_exists {
        format!("index missing; {archive_messages} archived messages to restore")
    } else if !integrity_ok {
        "integrity_check failed".into()
    } else if let Some(rows) = index_rows {
        format!("row drift: index {rows} vs archive {archive_messages}")
    } else {
        format!("messages table unreadable; {archive_messages} archived")
    }
}

/// Reconstruct outcome (testable; mapped to an exit code at the edge).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconstructExit {
    /// All projects healthy or rebuilt successfully.
    Ok,
    /// At least one project failed to rebuild (quarantine preserved).
    Failed,
    /// Nothing to do: data dir missing/unset, or no project archives found.
    Diagnostic,
}

/// Pure decision over the reconstruct results (ADR-0004 Ship 3 exit-code
/// discipline: 0 ok · 1 failed · 3 diagnostic).
fn classify_exit(
    reports: &[ReconstructReport],
    resolved: bool,
    project_count: usize,
) -> ReconstructExit {
    if !resolved || project_count == 0 {
        return ReconstructExit::Diagnostic;
    }
    if reports.iter().any(|r| r.status == "failed") {
        return ReconstructExit::Failed;
    }
    ReconstructExit::Ok
}

/// `toron doctor reconstruct` entry point (sync; spawns a runtime for the
/// async sqlx/rebuild work).
pub fn run_doctor_reconstruct(
    cfg: &Config,
    args: &DoctorReconstructArgs,
    format: OutputFormat,
) -> anyhow::Result<ExitCode> {
    let data_dir = resolve_data_dir(cfg, args.data_dir.as_deref());
    let resolved = data_dir.as_ref().map(|d| d.is_dir()).unwrap_or(false);
    let projects = if resolved {
        enumerate_projects(data_dir.as_ref().unwrap(), args.project.as_deref())
    } else {
        Vec::new()
    };

    let mut reports = Vec::with_capacity(projects.len());
    if resolved && !projects.is_empty() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        for root in &projects {
            reports.push(rt.block_on(reconstruct_project(root, args.dry_run))?);
        }
    }

    let outcome = classify_exit(&reports, resolved, projects.len());

    match format {
        OutputFormat::Json | OutputFormat::Plain => {
            let value = if resolved && !projects.is_empty() {
                json!({ "dry_run": args.dry_run, "reports": serde_json::to_value(&reports)? })
            } else {
                json!({
                    "status": "diagnostic",
                    "data_dir": data_dir.as_ref().map(|d| d.display().to_string()),
                    "error": if !resolved {
                        "no data dir (pass --data-dir, set TORON_DATA_DIR, or configure data_dir)".to_string()
                    } else {
                        "no project archives found (no subdirs with a messages/ dir)".to_string()
                    },
                })
            };
            render_json(&value, format);
        }
        OutputFormat::Human => {
            if !resolved || projects.is_empty() {
                let detail = if !resolved {
                    "no data dir located".to_string()
                } else {
                    format!(
                        "no project archives under {}",
                        data_dir.as_ref().unwrap().display()
                    )
                };
                println!("toron doctor reconstruct — diagnostic: {detail}");
            } else {
                println!(
                    "toron doctor reconstruct{}",
                    if args.dry_run { " (dry-run)" } else { "" }
                );
                for r in &reports {
                    let detail = match r.status.as_str() {
                        "healthy" => format!(
                            "{} archived, {} rows — no action",
                            r.archive_messages,
                            r.index_rows_before.unwrap_or(-1)
                        ),
                        "would_rebuild" => format!(
                            "would rebuild {} archived ({})",
                            r.archive_messages,
                            r.reason.as_deref().unwrap_or("")
                        ),
                        "rebuilt" => format!(
                            "rebuilt {} rows from {} archived",
                            r.rebuilt_rows.unwrap_or(0),
                            r.archive_messages
                        ),
                        "failed" => format!("FAILED: {}", r.reason.as_deref().unwrap_or("unknown")),
                        _ => r.status.clone(),
                    };
                    let q = r
                        .quarantine
                        .as_deref()
                        .map(|p| format!("  forensic: {p}"))
                        .unwrap_or_default();
                    println!("  [{:>12}] {} — {}{}", r.status, r.slug, detail, q);
                }
            }
        }
    }

    Ok(match outcome {
        ReconstructExit::Ok => ExitCode::SUCCESS,
        ReconstructExit::Failed => ExitCode::from(1),
        ReconstructExit::Diagnostic => ExitCode::from(3),
    })
}

/// Per-process counter state, persisted to a JSON file so an operator can
/// observe cumulative tool usage across invocations. The file is only written
/// on `record_*`; `snapshot` reads the in-process cache loaded at first use.
#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricsState {
    /// Successful tool calls keyed by tool name (e.g. "mail.inbox").
    pub call_counts: BTreeMap<String, u64>,
    /// Failed tool calls (recorded with the tool name they errored on).
    pub errors: u64,
    /// Per-call latency in microseconds (a small in-process histogram).
    pub latencies_us: Vec<u64>,
}

impl MetricsState {
    fn record(&mut self, name: &str, error: bool, duration: Duration) {
        if error {
            self.errors += 1;
        } else {
            *self.call_counts.entry(name.to_string()).or_insert(0) += 1;
        }
        self.latencies_us.push(duration.as_micros() as u64);
    }

    fn total_calls(&self) -> u64 {
        self.call_counts.values().sum::<u64>() + self.errors
    }
}

static STATE: OnceLock<Mutex<MetricsState>> = OnceLock::new();

/// Metrics file location: `TORON_METRICS_FILE` env override, else
/// `$TMPDIR/toron-cli-metrics.json`. Tests isolate via the env var.
fn metrics_file_path() -> PathBuf {
    std::env::var_os("TORON_METRICS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("toron-cli-metrics.json"))
}

fn state() -> &'static Mutex<MetricsState> {
    STATE.get_or_init(|| {
        let loaded = if let Ok(raw) = std::fs::read_to_string(metrics_file_path())
            && let Ok(parsed) = serde_json::from_str::<MetricsState>(&raw)
        {
            parsed
        } else {
            MetricsState::default()
        };
        Mutex::new(loaded)
    })
}

fn persist() {
    let path = metrics_file_path();
    let raw = match state().lock() {
        Ok(g) => serde_json::to_string(&*g).unwrap_or_else(|_| "{}".to_string()),
        Err(p) => serde_json::to_string(&*p.into_inner()).unwrap_or_else(|_| "{}".to_string()),
    };
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, raw).is_ok() {
        let _ = std::fs::rename(&tmp, &path);
    }
}

/// Record a successful robot tool call (tool name + duration).
pub fn record_tool_call(name: &str, duration: Duration) {
    {
        let mut guard = match state().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.record(name, false, duration);
    }
    persist();
}

/// Record a failed robot tool call (tool name + duration).
pub fn record_tool_error(name: &str, duration: Duration) {
    {
        let mut guard = match state().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        guard.record(name, true, duration);
    }
    persist();
}

/// JSON snapshot: `{tool_calls, errors, total_calls, latency_us}`.
pub fn snapshot() -> serde_json::Value {
    let (tool_calls, total, lat_count, lat_sum, errors) = {
        let guard = match state().lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        };
        let mut tool_calls = serde_json::Map::new();
        for (name, count) in &guard.call_counts {
            tool_calls.insert(name.clone(), json!(count));
        }
        (
            tool_calls,
            guard.total_calls(),
            guard.latencies_us.len(),
            guard.latencies_us.iter().sum::<u64>(),
            guard.errors,
        )
    };
    json!({
        "tool_calls": tool_calls,
        "errors": errors,
        "total_calls": total,
        "latency_us": { "count": lat_count, "sum": lat_sum },
    })
}

pub fn run_metrics() -> anyhow::Result<()> {
    print_json(&snapshot())
}

/// The MCP-tool-style name for a robot command (e.g. "mail.inbox"), used as
/// the metrics key. Non-robot commands never reach `run_robot`.
pub const fn robot_tool_name(command: &Command) -> &'static str {
    match command {
        Command::Mail(MailCommand::Send(_)) => "mail.send",
        Command::Mail(MailCommand::Reply(_)) => "mail.reply",
        Command::Mail(MailCommand::Inbox(_)) => "mail.inbox",
        Command::Mail(MailCommand::Ack(_)) => "mail.ack",
        Command::Mail(MailCommand::Read(_)) => "mail.read",
        Command::Mail(MailCommand::Search(_)) => "mail.search",
        Command::Mail(MailCommand::ThreadSummarize(_)) => "mail.thread_summarize",
        Command::Mail(MailCommand::Whois(_)) => "mail.whois",
        Command::AckSla(_) => "mail.ack_sla",
        Command::Reserve(ReserveCommand::Acquire(_)) => "reserve.acquire",
        Command::Reserve(ReserveCommand::Conflicts(_)) => "reserve.conflicts",
        Command::Reserve(ReserveCommand::List(_)) => "reserve.list",
        Command::Reserve(ReserveCommand::Renew(_)) => "reserve.renew",
        Command::Reserve(ReserveCommand::Release(_)) => "reserve.release",
        Command::Reserve(ReserveCommand::ReleaseByReason(_)) => "reserve.release_by_reason",
        Command::Reserve(ReserveCommand::Sweep(_)) => "reserve.sweep",
        Command::Reserve(ReserveCommand::Yield(_)) => "reserve.yield",
        Command::Reserve(ReserveCommand::Yields(_)) => "reserve.yields",
        Command::Reserve(ReserveCommand::ForceRelease(_)) => "reserve.force_release",
        Command::Reserve(ReserveCommand::RequestRelease(_)) => "reserve.request_release",
        Command::Reserve(ReserveCommand::Respond(_)) => "reserve.respond",
        Command::Reserve(ReserveCommand::Transfer(_)) => "reserve.transfer",
        Command::Reserve(ReserveCommand::WithLease(_)) => "reserve.with_lease",
        Command::Slot(SlotCommand::Acquire(_)) => "slot.acquire",
        Command::Slot(SlotCommand::Renew(_)) => "slot.renew",
        Command::Slot(SlotCommand::Release(_)) => "slot.release",
        Command::Contact(ContactCommand::Request(_)) => "contact.request",
        Command::Contact(ContactCommand::Respond(_)) => "contact.respond",
        Command::Contact(ContactCommand::List(_)) => "contact.list",
        Command::Contact(ContactCommand::Policy(_)) => "contact.policy",
        Command::Contact(ContactCommand::Captain(_)) => "contact.captain",
        Command::Guard(GuardCommand::Install(_)) => "guard.install",
        Command::Guard(GuardCommand::Uninstall(_)) => "guard.uninstall",
        Command::Agent(AgentCommand::Bootstrap(_)) => "agent.bootstrap",
        Command::Agent(AgentCommand::Prune(_)) => "agent.prune",
        _ => "unknown",
    }
}

/// Default key path for the AGENT and ANCHOR roles:
/// `~/.config/toron/<role>_nsec` (no HOME → relative).
///
/// The operator role does not come through here (ADR-0038) — it resolves via
/// [`toron_core::config::operator_key_path`], which also honors
/// `XDG_CONFIG_HOME`. Only the operator path is reconciled so far; the other
/// two roles still follow HOME alone.
fn default_key_path(role: &str) -> PathBuf {
    let home = toron_core::config::home_env("HOME").unwrap_or_default();
    home.join(".config")
        .join("toron")
        .join(format!("{role}_nsec"))
}

pub fn run_key(cfg: &Config, cmd: &KeyCommand) -> anyhow::Result<()> {
    match cmd {
        KeyCommand::Generate(args) => run_key_generate(cfg, args),
        KeyCommand::Inspect(args) => run_key_inspect(cfg, args),
    }
}

fn run_key_generate(cfg: &Config, args: &KeyGenerateArgs) -> anyhow::Result<()> {
    let path = args.out.clone().unwrap_or_else(|| match args.role {
        crate::cli::RoleArg::Operator => cfg
            .operator_key
            .clone()
            .unwrap_or_else(toron_core::config::operator_key_path),
        crate::cli::RoleArg::Agent => default_key_path("agent"),
        crate::cli::RoleArg::Anchor => default_key_path("anchor"),
    });
    if path.exists() {
        anyhow::bail!(
            "refusing to overwrite the existing key at {} — move or remove it first, or \
             generate to a different path with --out",
            path.display()
        );
    }
    let keys = toron_core::keys::generate();
    let password = key_password();
    toron_core::keys::write_encrypted_key(&path, &keys, &password, KEY_LOG_N)?;
    print_json(&json!({
        "npub": keys.public_key().to_hex(),
        "path": path.display().to_string(),
    }))
}

fn run_key_inspect(cfg: &Config, args: &KeyInspectArgs) -> anyhow::Result<()> {
    let path = match &args.path {
        Some(p) => p.clone(),
        None => cfg
            .operator_key
            .clone()
            .unwrap_or_else(toron_core::config::operator_key_path),
    };
    let password = key_password();
    let keys = toron_core::keys::load_encrypted_key(&path, &password)?;
    print_json(&json!({ "npub": keys.public_key().to_hex() }))
}

fn run_project_list() -> anyhow::Result<()> {
    let cfg = Config::load(None)?;
    let Some(data_dir) = resolve_data_dir(&cfg, None) else {
        anyhow::bail!("no data dir");
    };
    let mut projects: Vec<String> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&data_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                projects.push(entry.file_name().to_string_lossy().into_owned());
            }
        }
    }
    projects.sort();
    let note = if data_dir.is_dir() {
        format!("local listing of data dir ({})", data_dir.display())
    } else {
        "registry not wired".to_string()
    };
    print_json(&json!({ "projects": projects, "note": note }))
}

/// `project ensure` needs the demo mailbox (kind-30010 manifest signing), so it
/// is async via `#[tokio::main]` like the robot commands.
#[tokio::main]
async fn run_project_ensure(args: &ProjectEnsureArgs) -> anyhow::Result<()> {
    let mailbox = toron_mcp::demo_mailbox().await?;
    let state = mailbox.ensure_project(&args.slug).await?;
    print_json(&state.to_json())
}

pub fn run_project(cmd: &ProjectCommand) -> anyhow::Result<()> {
    match cmd {
        ProjectCommand::Ensure(args) => run_project_ensure(args),
        ProjectCommand::List => run_project_list(),
    }
}

pub fn run_relay(cfg: &Config, cmd: &RelayCommand) -> anyhow::Result<()> {
    match cmd {
        RelayCommand::List => print_json(&json!({ "relays": cfg.relays })),
        RelayCommand::Add(RelayUrlArg { url }) => {
            let mut relays = cfg.relays.clone();
            if !relays.iter().any(|r| r == url) {
                relays.push(url.clone());
            }
            print_json(&json!({
                "relays": relays,
                "note": "persist via .toron.toml (config write-back not wired)",
            }))
        }
        RelayCommand::Remove(RelayUrlArg { url }) => {
            let relays: Vec<String> = cfg.relays.iter().filter(|r| *r != url).cloned().collect();
            print_json(&json!({
                "relays": relays,
                "note": "persist via .toron.toml (config write-back not wired)",
            }))
        }
        RelayCommand::Info(RelayUrlArg { url }) => print_json(&json!({
            "url": url,
            "note": "NIP-11 fetch not wired (bd-led.3.4+)",
        })),
    }
}

pub fn run_backend(cfg: &Config, cmd: &BackendCommand) -> anyhow::Result<()> {
    use toron_plane::backend::plane::{Plane, PlaneChoice, PlaneEnv};

    let configured = cfg.effective_backend();
    let rex_bin_dir = std::env::var_os("REX_BIN_DIR")
        .filter(|value| !value.is_empty())
        .map(std::path::PathBuf::from);
    let resolved =
        toron_plane::backend::plane::resolve(&PlaneEnv::detect(configured, rex_bin_dir.as_deref()));
    if resolved.plane == Plane::Invalid {
        anyhow::bail!("{}", resolved.reason);
    }
    match cmd {
        BackendCommand::List => {
            let hc = toron_plane::backend::hc::HcBackend::connect(None).is_ok();
            print_json(&json!([
                { "name": "hc", "available": hc, "role": "fallback" },
                {
                    "name": "rex",
                    "available": resolved.rex_reachable,
                    "role": "primary when available",
                },
            ]))
        }
        BackendCommand::Status => print_json(&json!({
            "backend": configured,
            "serving": resolved.serving(),
            "rex_reachable": resolved.rex_reachable,
            "fallback": resolved.is_fallback(),
            "reason": resolved.reason,
        })),
        BackendCommand::Default(args) => print_json(&json!({
            "backend": PlaneChoice::parse(args.backend.as_str()).as_str(),
            "effective": cfg.effective_backend(),
            "note": "no persistence; set TORON_BACKEND or .toron.toml",
        })),
    }
}

#[cfg(test)]
mod backend_selection_tests {
    //! Plane selection is a clean break: `auto` | `rex` | `hc`, no legacy alias.
    use super::*;

    fn cfg_with(backend: &str) -> Config {
        Config {
            backend: Some(backend.into()),
            ..Config::default()
        }
    }

    #[test]
    fn legacy_backend_is_refused_naming_the_choices() {
        let err = run_backend(&cfg_with("herdr"), &BackendCommand::Status)
            .expect_err("legacy herdr must be refused");
        assert!(err.to_string().contains("want auto, rex, or hc"), "{err}");
    }

    #[test]
    fn hc_passes_backend_validation() {
        run_backend(&cfg_with("hc"), &BackendCommand::Status).expect("hc must validate");
    }

    /// `auto` is the default plane: it validates without any live socket.
    #[test]
    fn auto_passes_backend_validation() {
        run_backend(&cfg_with("auto"), &BackendCommand::Status).expect("auto must validate");
    }
}
