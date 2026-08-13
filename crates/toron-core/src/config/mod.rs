// governed-by: ADR-0007
#![forbid(unsafe_code)]

//! Durable data-root configuration (ADR-0007 D3 / R2).
//!
//! The mailbox data root is a **stable, shared, single-operator** directory:
//!
//! - default: `$XDG_DATA_HOME/toron` (fallback `~/.local/share/toron`), or
//!   the `TORON_DATA_DIR` env override — **never** a per-pid temp dir.
//! - layout:
//!   - `operator/operator.ncryptsec` — the operator key, mode **0600**
//!   - `projects/<slug>/` — per-project mailbox roots (archive + agents;
//!     the `Mailbox` data dir points at `projects/` directly)
//!   - `index.sqlite` — the shared SQLite index pool at the root
//!
//! ## Threat note (ADR-0007 D3)
//!
//! The old temp-per-pid roots were accidental isolation: every daemon got a
//! fresh key set and inbox. A shared root means **any leaf on the host can
//! read the same mailbox state** (mail, agent keys, operator key). This is
//! acceptable for a **single-operator, local-first** deployment — the operator
//! user owns the whole host. Multi-tenant hosts must NOT share one root.
//!
//! ## R2.0 gate
//!
//! Durable defaults only land in production paths because the R2.0 content-
//! keyed unlock cache bounds rebuild KDF cost to O(Δkeys). Tests and demos
//! may still use per-invocation dirs (`TORON_DEMO=1` / `TORON_MCP_DATA_DIR`)
//! for isolation.

use std::path::PathBuf;

/// Relative path of the operator key inside the data root.
pub const OPERATOR_KEY_RELATIVE: &str = "operator/operator.ncryptsec";
/// Relative path of the per-project mailbox roots directory.
pub const PROJECTS_RELATIVE: &str = "projects";
/// Index pool file name at the data root (shared across projects).
pub const INDEX_SQLITE: &str = "index.sqlite";
/// scrypt log-n for the durable operator key (P3 bd-p3-argon2-kdf-dos-bound-7sbu:
/// raised 12 -> 14 — 16x weaker than agent keys at 12 was a boot-speed tradeoff
/// rejected for the most privileged key; decrypt is payload-driven so existing
/// log_n=12 keys keep working).
pub const OPERATOR_KEY_LOG_N: u8 = 14;

/// Ambient environment inputs to data-root resolution. Pure struct so
/// resolution is unit-testable without mutating the process-global env.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DataRootEnv {
    pub toron_data_dir: Option<PathBuf>,
    pub xdg_data_home: Option<PathBuf>,
    pub home: Option<PathBuf>,
}

impl DataRootEnv {
    /// Snapshot the ambient environment.
    pub fn from_ambient() -> Self {
        Self {
            toron_data_dir: env_non_empty("TORON_DATA_DIR"),
            xdg_data_home: env_non_empty("XDG_DATA_HOME"),
            home: env_non_empty("HOME"),
        }
    }
}

fn env_non_empty(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// Resolve the durable data root (ADR-0007 D3): `TORON_DATA_DIR` >
/// `$XDG_DATA_HOME/toron` > `~/.local/share/toron`. Last resort is a
/// relative `toron-data` dir — still never a per-pid temp root.
pub fn resolve_data_root(env: &DataRootEnv) -> PathBuf {
    if let Some(dir) = &env.toron_data_dir {
        return dir.clone();
    }
    if let Some(xdg) = &env.xdg_data_home {
        return xdg.join("toron");
    }
    if let Some(home) = &env.home {
        return home.join(".local").join("share").join("toron");
    }
    PathBuf::from("toron-data")
}

/// The durable data root from the ambient environment.
pub fn durable_data_dir() -> PathBuf {
    resolve_data_root(&DataRootEnv::from_ambient())
}

/// The durable data-root layout (ADR-0007 D3).
///
/// `root` is the mailbox data dir: project roots hang directly off it as
/// `root/projects/<slug>/` is NOT the mailbox convention — the Mailbox
/// treats `root` as the parent of project roots, so `projects/` is passed as
/// the Mailbox `data_dir` and projects land at `projects/<slug>/`.
#[derive(Debug, Clone)]
pub struct ToronPaths {
    /// The durable data root.
    pub root: PathBuf,
}

impl ToronPaths {
    /// Resolve the durable root from the ambient environment.
    pub fn resolve() -> Self {
        Self {
            root: durable_data_dir(),
        }
    }

    /// True when the process wants a unique temp dir per call (demo/tests).
    /// Set `TORON_DEMO=1` — bounds KDF scans; cross-process tests should set
    /// `TORON_MCP_DATA_DIR` to a shared path instead.
    pub fn demo_mode() -> bool {
        env_non_empty("TORON_DEMO").is_some()
    }

    /// Create `operator/` + `projects/` under the root (idempotent).
    pub fn ensure_layout(&self) -> anyhow::Result<()> {
        std::fs::create_dir_all(self.operator_dir())?;
        std::fs::create_dir_all(self.projects_dir())?;
        Ok(())
    }

    /// Best-effort chmod 0600 of an existing operator key. No-op when the
    /// key file is absent (generation writes 0600 already).
    pub fn ensure_operator_key_mode(&self) -> anyhow::Result<()> {
        let path = self.operator_key_path();
        if path.is_file() {
            crate::keys::write_encrypted_key(
                &path,
                &crate::keys::load_encrypted_key(&path, &operator_passphrase())?,
                &operator_passphrase(),
                OPERATOR_KEY_LOG_N,
            )?;
        }
        Ok(())
    }

    /// `operator/` — holds the operator key (0600).
    pub fn operator_dir(&self) -> PathBuf {
        self.root.join("operator")
    }

    /// `operator/operator.ncryptsec`.
    pub fn operator_key_path(&self) -> PathBuf {
        self.root.join(OPERATOR_KEY_RELATIVE)
    }

    /// `projects/` — the `Mailbox` data dir (project roots hang off it).
    pub fn projects_dir(&self) -> PathBuf {
        self.root.join(PROJECTS_RELATIVE)
    }

    /// `projects/<slug>/` — one mailbox root per project.
    pub fn project_dir(&self, slug: &str) -> PathBuf {
        self.projects_dir().join(slug)
    }

    /// `<root>/index.sqlite` — the shared SQLite index pool.
    pub fn index_path(&self) -> PathBuf {
        self.root.join(INDEX_SQLITE)
    }

    /// Load the operator key (`operator/operator.ncryptsec`, mode 0600) or
    /// generate + write it on first use.
    ///
    /// Idempotent across processes sharing one root: the second opener loads
    /// the SAME key instead of minting a new one — the R2 "two processes
    /// share state" unit equivalent for the operator identity.
    pub fn ensure_operator_key(&self, passphrase: &str) -> anyhow::Result<crate::keys::Keys> {
        let path = self.operator_key_path();
        if path.is_file() {
            return crate::keys::load_encrypted_key(&path, passphrase)
                .map_err(|e| anyhow::anyhow!("operator key {}: {e}", path.display()));
        }
        let keys = crate::keys::generate();
        crate::keys::write_encrypted_key(&path, &keys, passphrase, OPERATOR_KEY_LOG_N)
            .map_err(|e| anyhow::anyhow!("write operator key {}: {e}", path.display()))?;
        Ok(keys)
    }
}

/// The operator key passphrase — resolved in priority order (see below). The
/// passphrase file keeps the secret OUT of launchd/systemd unit files:
/// `toron daemon install` writes the passphrase file at
/// `~/.config/toron/passphrase` (mode 0600) and the unit references only the
/// key path.
///
/// Resolution order:
/// 1. `TORON_KEY_PASSPHRASE` (explicit env — dev/CI).
/// 2. `TORON_KEY_PASSPHRASE_FILE` — a path; its trimmed content is used.
/// 3. First existing `$XDG_CONFIG_HOME/toron/passphrase` /
///    `~/.config/toron/passphrase` (daemon installs).
/// 4. `"toron"` (dev default; MUST agree with `key generate`'s fallback).
pub fn operator_passphrase() -> String {
    resolve_passphrase(
        std::env::var("TORON_KEY_PASSPHRASE").ok().as_deref(),
        std::env::var_os("TORON_KEY_PASSPHRASE_FILE")
            .map(std::path::PathBuf::from)
            .as_deref(),
        &passphrase_file_candidates(
            env_non_empty("XDG_CONFIG_HOME").as_deref(),
            env_non_empty("HOME").as_deref(),
        ),
    )
}

/// Config-home passphrase file candidates in priority order (pure).
pub fn passphrase_file_candidates(
    xdg_config_home: Option<&std::path::Path>,
    home: Option<&std::path::Path>,
) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Some(x) = xdg_config_home {
        v.push(x.join("toron").join("passphrase"));
    }
    if let Some(h) = home {
        v.push(h.join(".config").join("toron").join("passphrase"));
    }
    v
}

/// Pure passphrase resolution (env inputs passed in — unit-testable).
pub fn resolve_passphrase(
    env_value: Option<&str>,
    file_env: Option<&std::path::Path>,
    candidates: &[PathBuf],
) -> String {
    if let Some(p) = env_value.filter(|p| !p.is_empty()) {
        return p.to_string();
    }
    if let Some(f) = file_env
        && let Ok(s) = std::fs::read_to_string(f)
        && !s.trim().is_empty()
    {
        return s.trim().to_string();
    }
    for c in candidates {
        if let Ok(s) = std::fs::read_to_string(c) {
            let t = s.trim();
            if !t.is_empty() {
                return t.to_string();
            }
        }
    }
    "toron".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static SEQ: AtomicU32 = AtomicU32::new(0);

    fn scratch(name: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("toron-r2-{name}-{}-{n}", std::process::id()))
    }

    #[test]
    fn env_override_beats_xdg() {
        let env = DataRootEnv {
            toron_data_dir: Some(PathBuf::from("/env/root")),
            xdg_data_home: Some(PathBuf::from("/xdg")),
            home: Some(PathBuf::from("/home")),
        };
        assert_eq!(resolve_data_root(&env), PathBuf::from("/env/root"));
    }

    #[test]
    fn xdg_data_home_wins_over_home_fallback() {
        let env = DataRootEnv {
            toron_data_dir: None,
            xdg_data_home: Some(PathBuf::from("/xdg")),
            home: Some(PathBuf::from("/home")),
        };
        assert_eq!(resolve_data_root(&env), PathBuf::from("/xdg/toron"));
    }

    #[test]
    fn home_fallback_is_local_share_toron() {
        let env = DataRootEnv {
            toron_data_dir: None,
            xdg_data_home: None,
            home: Some(PathBuf::from("/home/alice")),
        };
        assert_eq!(
            resolve_data_root(&env),
            PathBuf::from("/home/alice/.local/share/toron")
        );
    }

    #[test]
    fn no_home_falls_back_to_relative_never_temp_pid() {
        let env = DataRootEnv::default();
        let root = resolve_data_root(&env);
        assert_eq!(root, PathBuf::from("toron-data"));
        assert!(
            !root.starts_with(std::env::temp_dir()),
            "never a temp per-pid root"
        );
    }

    #[test]
    fn layout_paths_follow_d3() {
        let paths = ToronPaths {
            root: PathBuf::from("/root"),
        };
        assert_eq!(paths.operator_dir(), PathBuf::from("/root/operator"));
        assert_eq!(
            paths.operator_key_path(),
            PathBuf::from("/root/operator/operator.ncryptsec")
        );
        assert_eq!(paths.projects_dir(), PathBuf::from("/root/projects"));
        assert_eq!(
            paths.project_dir("demo"),
            PathBuf::from("/root/projects/demo")
        );
        assert_eq!(paths.index_path(), PathBuf::from("/root/index.sqlite"));
    }

    #[test]
    fn ensure_layout_creates_operator_and_projects() {
        let root = scratch("dirs");
        let paths = ToronPaths { root: root.clone() };
        paths.ensure_layout().expect("create dirs");
        assert!(paths.operator_dir().is_dir());
        assert!(paths.projects_dir().is_dir());
        paths.ensure_layout().expect("idempotent");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn operator_key_created_0600_and_idempotent_across_openers() {
        // Two "processes" opening the same root must see the SAME operator
        // key (the R2 two-process sharing unit equivalent).
        let root = scratch("opkey");
        let paths = ToronPaths { root: root.clone() };
        paths.ensure_layout().expect("dirs");

        let first = paths.ensure_operator_key("toron").expect("first open");
        let path = paths.operator_key_path();
        assert!(path.is_file(), "key file exists");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).expect("meta").permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "operator key is 0600");
        }

        let second = paths.ensure_operator_key("toron").expect("second open");
        assert_eq!(
            first.public_key(),
            second.public_key(),
            "second opener loads the SAME key (no re-mint)"
        );

        // Wrong passphrase fails loudly, never silently re-mints.
        assert!(
            paths.ensure_operator_key("wrong").is_err(),
            "passphrase mismatch errors"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn index_path_is_stable_not_pid_scoped() {
        let p1 = ToronPaths {
            root: PathBuf::from("/root"),
        }
        .index_path();
        let p2 = ToronPaths {
            root: PathBuf::from("/root"),
        }
        .index_path();
        assert_eq!(p1, p2, "stable path, no pid suffix");
    }

    /// P3 (bd-p3-argon2-kdf-dos-bound-7sbu): the OPERATOR key is the most
    /// privileged key (Overseer bypass, manifests, force-release) — its KDF
    /// must not be materially weaker than the agent-key standard. Agent keys
    /// use NIP-49 scrypt log_n=16; the operator key must stay within one
    /// order of magnitude (>= 14; 12 = 16x weaker, boot-speed tradeoff
    /// rejected). Decrypt is payload-driven, so existing log_n=12 keys keep
    /// working after the bump.
    #[test]
    #[allow(clippy::assertions_on_constants)] // guard against future KDF weakening
    fn operator_key_kdf_not_weak() {
        assert!(
            OPERATOR_KEY_LOG_N >= 14,
            "operator key scrypt log_n={OPERATOR_KEY_LOG_N} is 16x weaker than agent keys (16); must be >= 14"
        );
        assert!(
            crate::keys::DEFAULT_LOG_N >= 16,
            "agent-key scrypt log_n must stay NIP-49 spec-standard (>= 16), got {}",
            crate::keys::DEFAULT_LOG_N
        );
    }

    #[test]
    fn passphrase_env_beats_file_beats_default() {
        let d = scratch("pp");
        std::fs::create_dir_all(d.join("xdg").join("toron")).unwrap();
        std::fs::write(
            d.join("xdg").join("toron").join("passphrase"),
            "  from-xdg\n",
        )
        .unwrap();
        let file = d.join("override.pass");
        std::fs::write(&file, "from-file-env\n").unwrap();
        let candidates = passphrase_file_candidates(Some(&d.join("xdg")), None);
        assert_eq!(
            resolve_passphrase(Some("env-secret"), Some(&file), &candidates),
            "env-secret"
        );
        assert_eq!(
            resolve_passphrase(None, Some(&file), &candidates),
            "from-file-env"
        );
        assert_eq!(resolve_passphrase(None, None, &candidates), "from-xdg");
        assert_eq!(resolve_passphrase(None, None, &[]), "toron");
    }

    #[test]
    fn passphrase_env_file_missing_or_empty_falls_through() {
        let d = scratch("pp2");
        let candidates = passphrase_file_candidates(None, Some(&d.join("home")));
        assert_eq!(
            resolve_passphrase(None, Some(&d.join("missing.pass")), &candidates),
            "toron"
        );
        std::fs::create_dir_all(d.join("home").join(".config").join("toron")).unwrap();
        std::fs::write(
            d.join("home")
                .join(".config")
                .join("toron")
                .join("passphrase"),
            "   \n",
        )
        .unwrap();
        assert_eq!(
            resolve_passphrase(None, None, &candidates),
            "toron",
            "whitespace-only file is ignored"
        );
    }

    #[test]
    fn passphrase_candidates_prefer_xdg_then_home() {
        let v =
            passphrase_file_candidates(Some(&PathBuf::from("/xdg")), Some(&PathBuf::from("/home")));
        assert_eq!(v[0], PathBuf::from("/xdg/toron/passphrase"));
        assert_eq!(v[1], PathBuf::from("/home/.config/toron/passphrase"));
    }
}
