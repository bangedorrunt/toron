// governed-by: ADR-0007
// governed-by: ADR-0036 (daemon state dir — wake.sock discovery)
// governed-by: ADR-0038 (canonical operator key path — config home, not data root)
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

use std::path::{Path, PathBuf};

/// Relative path of the operator key inside the data root.
///
/// LEGACY (ADR-0038): the data root was where the operator key used to live,
/// and it is kept only so the guard that refuses to mint a second operator
/// identity can NAME the old location. Nothing loads a key from here.
pub const OPERATOR_KEY_RELATIVE: &str = "operator/operator.ncryptsec";
/// Canonical operator key file name inside `<config home>/toron/`.
///
/// The key is a SECRET, so it belongs beside the passphrase (same directory,
/// same 0600 discipline) rather than in the data root with the bus and the
/// search index. This is also the path `toron key generate --role operator`
/// writes and the path the docs tell an operator to export (ADR-0038).
pub const OPERATOR_KEY_BASENAME: &str = "operator_nsec";
/// Relative path of the per-project mailbox roots directory.
pub const PROJECTS_RELATIVE: &str = "projects";
/// Index pool file name at the data root (shared across projects).
pub const INDEX_SQLITE: &str = "index.sqlite";
/// scrypt log-n for the durable operator key (P3 bd-p3-argon2-kdf-dos-bound-7sbu:
/// raised 12 -> 14 — 16x weaker than agent keys at 12 was a boot-speed tradeoff
/// rejected for the most privileged key; decrypt is payload-driven so existing
/// log_n=12 keys keep working).
pub const OPERATOR_KEY_LOG_N: u8 = 14;

/// The environment marker a test harness sets on every `toron` it spawns.
///
/// Its value is the harness's sandbox directory. A process that carries it is
/// under test: it never resolves the operator's data root, config home or
/// state home from an inherited `HOME` / `XDG_*`, and anything it does not
/// have an explicit pin for goes under the marker's directory instead.
///
/// The marker is explicit because nothing else is reliable. The spawned child
/// is called `toron`, exactly like the installed binary, and where a build
/// tool puts it is the build tool's business (`mbx` puts it where no cargo
/// marker file sits above it). On 2026-10-24 such a child, run with the
/// operator's `HOME` and `XDG_DATA_HOME`, migrated the live store to a schema
/// no installed binary knew. `toron_core::testkit::toron_command` sets it.
pub const UNDER_TEST_ENV: &str = "TORON_UNDER_TEST";

/// Ambient environment inputs to data-root resolution. Pure struct so
/// resolution is unit-testable without mutating the process-global env.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DataRootEnv {
    pub toron_data_dir: Option<PathBuf>,
    pub xdg_data_home: Option<PathBuf>,
    pub home: Option<PathBuf>,
    /// `Some` when this process is under test and must never land on the
    /// operator's live root. `None` for a production process.
    pub sandbox: Option<Sandbox>,
}

/// A process under test, and where its sandbox is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sandbox {
    pub kind: SandboxKind,
    /// The OS temp root. An implicit root (from `XDG_*` or `HOME`) under it,
    /// or under `/tmp`, is a fixture a test built, and is honoured. Any other
    /// implicit root is taken to be the operator's and is refused.
    pub temp_dir: PathBuf,
    /// The sandbox directory: the data root when nothing pins one, and the
    /// parent of the sandboxed home (`<root>/home`).
    pub root: PathBuf,
}

/// How a process knows it is under test. See [`Sandbox`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxKind {
    /// It carries [`UNDER_TEST_ENV`], set by the harness that spawned it.
    Marker,
    /// It is a test binary itself: `cfg(test)`, or a cargo test executable
    /// (`<crate>-<hash>`, bd-fize), which covers integration tests in-process.
    TestBinary,
}

impl Sandbox {
    /// The sandboxed home: what `HOME` means for a process under test whose
    /// inherited `HOME` is not scratch.
    pub fn home(&self) -> PathBuf {
        self.root.join("home")
    }

    /// True when `root` is scratch space a test built: under the OS temp root
    /// or `/tmp`, with no `..` that could climb back out.
    pub fn is_scratch(&self, root: &Path) -> bool {
        if root
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        {
            return false;
        }
        (!self.temp_dir.as_os_str().is_empty() && root.starts_with(&self.temp_dir))
            || root.starts_with("/tmp")
            || root.starts_with("/private/tmp")
    }
}

impl DataRootEnv {
    /// Snapshot the ambient environment.
    pub fn from_ambient() -> Self {
        Self {
            toron_data_dir: env_non_empty("TORON_DATA_DIR"),
            xdg_data_home: env_non_empty("XDG_DATA_HOME"),
            home: env_non_empty("HOME"),
            sandbox: ambient_sandbox(),
        }
    }
}

/// Pure: is a process with this marker value (and this test-binary verdict)
/// under test, and where is its sandbox? `pid` names the per-process sandbox
/// used when the marker gives no usable directory.
pub fn sandbox_for(
    marker: Option<&Path>,
    test_binary: bool,
    temp_dir: &Path,
    pid: u32,
) -> Option<Sandbox> {
    let per_process = temp_dir.join(format!("toron-test-root-{pid}"));
    if let Some(marker) = marker {
        let mut sandbox = Sandbox {
            kind: SandboxKind::Marker,
            temp_dir: temp_dir.to_path_buf(),
            root: per_process,
        };
        if marker.is_absolute() && sandbox.is_scratch(marker) {
            sandbox.root = marker.to_path_buf();
        }
        return Some(sandbox);
    }
    test_binary.then(|| Sandbox {
        kind: SandboxKind::TestBinary,
        temp_dir: temp_dir.to_path_buf(),
        root: per_process,
    })
}

/// This process's sandbox, or `None` in production.
pub fn ambient_sandbox() -> Option<Sandbox> {
    sandbox_for(
        env_non_empty(UNDER_TEST_ENV).as_deref(),
        cfg!(test) || is_test_process(),
        &std::env::temp_dir(),
        std::process::id(),
    )
}

/// Pure: what a home-shaped variable (`HOME`, `XDG_CONFIG_HOME`,
/// `XDG_DATA_HOME`, `XDG_STATE_HOME`, `XDG_CACHE_HOME`) means for a process.
///
/// In production it is the value. Under test, a scratch value is honoured (a
/// test's fixture home) and anything else is mapped into the sandboxed home,
/// so that every path derived from it (the operator key, the passphrase, the
/// state dir and wake.sock, the flywheel config, launchd / systemd unit dirs)
/// lands in the sandbox. An unset variable stays unset.
pub fn resolve_home_var(
    name: &str,
    value: Option<PathBuf>,
    sandbox: Option<&Sandbox>,
) -> Option<PathBuf> {
    let value = value?;
    let Some(sandbox) = sandbox else {
        return Some(value);
    };
    if sandbox.is_scratch(&value) {
        return Some(value);
    }
    let home = sandbox.home();
    Some(match name {
        "XDG_CONFIG_HOME" => home.join(".config"),
        "XDG_DATA_HOME" => home.join(".local").join("share"),
        "XDG_STATE_HOME" => home.join(".local").join("state"),
        "XDG_CACHE_HOME" => home.join(".cache"),
        _ => home,
    })
}

/// A home-shaped variable from the ambient environment, sandboxed under test.
/// See [`resolve_home_var`]. Every read of `HOME` / `XDG_*` in the workspace
/// goes through here.
pub fn home_env(name: &str) -> Option<PathBuf> {
    resolve_home_var(name, env_non_empty(name), ambient_sandbox().as_ref())
}

/// The sandboxed home when this process is under test, else `None`. For the
/// callers whose fallback (with the variable unset) is a fixed real path.
pub fn sandbox_home() -> Option<PathBuf> {
    ambient_sandbox().map(|s| s.home())
}

/// True when this process is a cargo TEST binary.
///
/// bd-fize: cargo names a test binary `<crate>-<hash>`; the production binary
/// has no hash suffix. That is the only in-process signal a test has, and it
/// is what keeps a test out of the operator's live store: a test that
/// registers an agent without pinning a data dir would otherwise write a
/// PERMANENT identity — a name that can be addressed, a pubkey that can
/// receive mail — into the operator's index, where it stays forever.
///
/// It says nothing about a `toron` a test SPAWNS: that one is named `toron`.
/// The spawned child is under test only through [`UNDER_TEST_ENV`].
pub fn is_test_process() -> bool {
    let Some(exe) = std::env::current_exe().ok() else {
        return false;
    };
    let Some(stem) = exe.file_stem().and_then(|s| s.to_str()) else {
        return false;
    };
    let Some((_, suffix)) = stem.rsplit_once('-') else {
        return false;
    };
    suffix.len() >= 8 && suffix.bytes().all(|b| b.is_ascii_hexdigit())
}

fn env_non_empty(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
}

/// A `TORON_*` knob that refused to parse: the name, what was set, and the
/// accepted forms. Parse-or-refuse (wave7-c): unknown is an error, never a
/// silent default.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{name}={value:?} is not valid (accepted: {accepted})")]
pub struct ConfigError {
    pub name: &'static str,
    pub value: String,
    pub accepted: &'static str,
}

/// One strict parse of a `TORON_*` knob: unset/empty → `Ok(None)` (the caller
/// applies its default); set → `parse` or a [`ConfigError`] naming `accepted`.
/// The single interpretation site per knob — domain code calls the typed
/// accessor, never `env::var` plus an ad-hoc parse.
pub fn parse_env_opt<T>(
    name: &'static str,
    accepted: &'static str,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Result<Option<T>, ConfigError> {
    let Some(os) = std::env::var_os(name).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let raw = os.to_string_lossy();
    parse(&raw).map(Some).ok_or_else(|| ConfigError {
        name,
        value: raw.into_owned(),
        accepted,
    })
}

/// `1|true|on` → true, `0|false|off` → false (ASCII case-insensitive).
pub fn parse_env_bool(name: &'static str) -> Result<Option<bool>, ConfigError> {
    parse_env_opt(name, "1|true|on|0|false|off", |raw| {
        match raw.to_ascii_lowercase().as_str() {
            "1" | "true" | "on" => Some(true),
            "0" | "false" | "off" => Some(false),
            _ => None,
        }
    })
}

/// A non-negative integer (leading/trailing whitespace tolerated).
pub fn parse_env_u64(name: &'static str) -> Result<Option<u64>, ConfigError> {
    parse_env_opt(name, "a non-negative integer", |raw| {
        raw.trim().parse::<u64>().ok()
    })
}

/// A non-empty path.
pub fn parse_env_path(name: &'static str) -> Result<Option<PathBuf>, ConfigError> {
    parse_env_opt(name, "a non-empty path", |raw| Some(PathBuf::from(raw)))
}

/// A non-empty string.
pub fn parse_env_string(name: &'static str) -> Result<Option<String>, ConfigError> {
    parse_env_opt(name, "a non-empty value", |raw| Some(raw.to_string()))
}

/// An `http://` or `https://` URL.
pub fn parse_env_url(name: &'static str) -> Result<Option<String>, ConfigError> {
    parse_env_opt(name, "an http:// or https:// URL", |raw| {
        (raw.starts_with("http://") || raw.starts_with("https://")).then(|| raw.to_string())
    })
}

/// Resolve the durable data root (ADR-0007 D3): `TORON_DATA_DIR` >
/// `$XDG_DATA_HOME/toron` > `~/.local/share/toron`. Last resort is a
/// relative `toron-data` dir — still never a per-pid temp root.
///
/// A process under test never resolves the operator's live root. Only
/// `TORON_DATA_DIR` is an explicit pin. `XDG_DATA_HOME` and `HOME` are
/// ambient: an operator's shell exports them, and every child inherits them.
/// So a process under test honours them only when they point into scratch
/// space (see [`Sandbox::is_scratch`]), which is how a test builds a fixture
/// home. Anywhere else it gets its sandbox root instead.
pub fn resolve_data_root(env: &DataRootEnv) -> PathBuf {
    if let Some(dir) = &env.toron_data_dir {
        return dir.clone();
    }
    let implicit = env
        .xdg_data_home
        .as_ref()
        .map(|xdg| xdg.join("toron"))
        .or_else(|| {
            env.home
                .as_ref()
                .map(|home| home.join(".local").join("share").join("toron"))
        });
    match &env.sandbox {
        None => implicit.unwrap_or_else(|| PathBuf::from("toron-data")),
        Some(sandbox) => match implicit {
            Some(root) if sandbox.is_scratch(&root) => root,
            _ => sandbox.root.clone(),
        },
    }
}

/// The durable data root from the ambient environment.
///
/// A spawned process under test that is sent to its sandbox says so on
/// stderr, once: its harness should have pinned `TORON_DATA_DIR`.
pub fn durable_data_dir() -> PathBuf {
    let env = DataRootEnv::from_ambient();
    let root = resolve_data_root(&env);
    if let Some(sandbox) = &env.sandbox
        && sandbox.kind == SandboxKind::Marker
        && root == sandbox.root
    {
        static WARNED: std::sync::Once = std::sync::Once::new();
        WARNED.call_once(|| {
            eprintln!(
                "toron: under test ({UNDER_TEST_ENV} is set) with no TORON_DATA_DIR, so it \
                 never opens the operator's data root; using {}.",
                root.display()
            );
        });
    }
    root
}

/// Pure: the canonical operator key path from config-home components.
///
/// `$XDG_CONFIG_HOME/toron/operator_nsec`, then
/// `~/.config/toron/operator_nsec`, then the bare relative name when there is
/// no home at all. Mirrors [`resolve_data_root`]'s shape so both resolutions
/// read the same way.
pub fn resolve_operator_key_path(
    xdg_config_home: Option<&std::path::Path>,
    home: Option<&std::path::Path>,
) -> PathBuf {
    if let Some(dir) = xdg_config_home {
        return dir.join("toron").join(OPERATOR_KEY_BASENAME);
    }
    if let Some(home) = home {
        return home
            .join(".config")
            .join("toron")
            .join(OPERATOR_KEY_BASENAME);
    }
    PathBuf::from(OPERATOR_KEY_BASENAME)
}

/// The canonical operator key path from the ambient environment (ADR-0038).
///
/// The ONE resolution for the operator key: the daemon unit, the MCP host's
/// implicit fallback, the CLI's `key generate` default, and `doctor` all read
/// this, so producer and consumers cannot disagree about which key is the
/// operator identity.
pub fn operator_key_path() -> PathBuf {
    resolve_operator_key_path(
        home_env("XDG_CONFIG_HOME").as_deref(),
        home_env("HOME").as_deref(),
    )
}

/// The pre-ADR-0038 operator key location (inside the data root).
///
/// Named so the mint guard and `doctor` can REPORT it. A key here is a
/// different key from the canonical one — on the machine this was written for,
/// the two resolved to different npubs — so it is never loaded implicitly and
/// never silently replaced.
pub fn legacy_operator_key_path() -> PathBuf {
    durable_data_dir().join(OPERATOR_KEY_RELATIVE)
}

/// What a host should do about the operator key. Pure, so the load/mint/refuse
/// rule is testable without a filesystem or a process environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperatorKeySource {
    /// The canonical file exists — load it.
    Load,
    /// No key anywhere — mint one at the canonical path.
    Mint,
    /// No canonical key, but a LEGACY data-root key exists. Refuse: minting
    /// would leave TWO operator identities on one machine, and operator-signed
    /// work would verify against whichever one the reader happened to resolve.
    RefuseLegacy,
}

/// The mint decision: only ever one operator identity per machine (ADR-0038).
pub const fn operator_key_source(canonical_exists: bool, legacy_exists: bool) -> OperatorKeySource {
    match (canonical_exists, legacy_exists) {
        (true, _) => OperatorKeySource::Load,
        (false, true) => OperatorKeySource::RefuseLegacy,
        (false, false) => OperatorKeySource::Mint,
    }
}

/// Set `path` to 0600 when it exists, and touch nothing else.
///
/// This used to re-ENCRYPT the key (load + `write_encrypted_key`) to reach the
/// same end, which is not a chmod: `write_encrypted_key` TRUNCATES the live
/// file, so a crash between truncate and write loses the identity, and every
/// host boot paid two log_n=14 scrypt passes for a permission bit. It now sets
/// the mode it claims to set.
///
/// Parameterized so the property that matters — the key BYTES are untouched —
/// is testable without the ambient config home. On a platform without POSIX
/// modes this is a no-op rather than a claim it cannot keep.
pub fn ensure_key_mode_at(path: &std::path::Path) -> anyhow::Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

/// Load-or-mint the operator key, with BOTH paths supplied by the caller.
///
/// The paths are parameters rather than ambient reads for two reasons. It makes
/// the mint/refuse rule testable without touching a real config home (a unit
/// test that resolved ambient paths would read — or MINT into — the operator's
/// own key), and it lets an isolated root (a test/demo data dir) keep its
/// operator key beside its own state instead of reaching out to the real one.
pub fn ensure_operator_key_at(
    canonical: &std::path::Path,
    legacy: &std::path::Path,
    passphrase: &str,
) -> anyhow::Result<crate::keys::Keys> {
    match operator_key_source(canonical.is_file(), legacy.is_file()) {
        OperatorKeySource::Load => crate::keys::load_encrypted_key(canonical, passphrase)
            .map_err(|e| anyhow::anyhow!("operator key {}: {e}", canonical.display())),
        OperatorKeySource::Mint => {
            let keys = crate::keys::generate();
            crate::keys::write_encrypted_key(canonical, &keys, passphrase, OPERATOR_KEY_LOG_N)
                .map_err(|e| anyhow::anyhow!("write operator key {}: {e}", canonical.display()))?;
            Ok(keys)
        }
        OperatorKeySource::RefuseLegacy => anyhow::bail!(
            "refusing to mint a second operator identity: no key at {}, but a LEGACY \
             key exists at {}. They are different keys, so operator-signed work would \
             verify against whichever one the reader resolved. Move the one you mean to \
             keep (ADR-0038): mv {} {}",
            canonical.display(),
            legacy.display(),
            legacy.display(),
            canonical.display()
        ),
    }
}

/// Ambient environment inputs to daemon **state**-dir resolution (pidfiles,
/// logs, `wake.sock`). Pure struct so resolution is unit-testable without
/// mutating the process-global env — mirrors [`DataRootEnv`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateDirEnv {
    pub xdg_state_home: Option<PathBuf>,
    pub home: Option<PathBuf>,
}

impl StateDirEnv {
    /// Snapshot the ambient environment.
    pub fn from_ambient() -> Self {
        Self {
            xdg_state_home: home_env("XDG_STATE_HOME"),
            home: home_env("HOME"),
        }
    }
}

/// Resolve the daemon STATE dir (distinct from the data root):
/// `$XDG_STATE_HOME/toron` > `~/.local/state/toron`. Last resort is the
/// relative `toron-state` name the CLI always used.
///
/// The single derivation for state-side discovery (ADR-0036 Spec 2,
/// bd-ewsi): pidfiles, logs, and `wake.sock` resolve through here — the
/// serve-side sidecar bind and the consumer-side discoveries (toron-cli
/// slice B, doorbell `discover_from`) share it, so producer and consumers
/// cannot disagree about the path by construction. Deriving independently
/// is the exact starvation this closes: every consumer dialed
/// `$XDG_STATE_HOME/toron/wake.sock` while nothing served it.
pub fn resolve_state_dir(env: &StateDirEnv) -> PathBuf {
    if let Some(dir) = &env.xdg_state_home {
        return dir.join("toron");
    }
    if let Some(home) = &env.home {
        return home.join(".local").join("state").join("toron");
    }
    PathBuf::from("toron-state")
}

/// The daemon state dir from the ambient environment.
pub fn state_dir() -> PathBuf {
    resolve_state_dir(&StateDirEnv::from_ambient())
}

/// Root-level file dir for a mailbox `data_dir` (bd-m5ym).
///
/// Production mailboxes are wired with `data_dir = <root>/projects`
/// (the `Mailbox` treats its `data_dir` as the parent of project roots),
/// but root-level files (`serve.pid`, `presence.jsonl`, `index.sqlite`)
/// belong at `<root>/`. Demo/test mailboxes use `data_dir` as the root
/// itself, so it is returned unchanged.
pub fn mailbox_root(data_dir: &std::path::Path) -> PathBuf {
    if data_dir
        .file_name()
        .map(|n| n == "projects")
        .unwrap_or(false)
        && let Some(parent) = data_dir.parent()
    {
        return parent.to_path_buf();
    }
    data_dir.to_path_buf()
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

    /// Best-effort chmod 0600 of an existing operator key. No-op when the key
    /// file is absent (generation writes 0600 already).
    pub fn ensure_operator_key_mode(&self) -> anyhow::Result<()> {
        ensure_key_mode_at(&self.operator_key_path())
    }

    /// `operator/` — holds the operator key (0600).
    pub fn operator_dir(&self) -> PathBuf {
        self.root.join("operator")
    }

    /// The canonical operator key path (ADR-0038).
    ///
    /// Deliberately does NOT use `self.root`: the key is a secret and lives in
    /// the config home beside the passphrase, not in the data root. Kept as a
    /// method so existing callers (`daemon install`, the mailbox host) do not
    /// move, but there is exactly ONE resolution behind it — see
    /// [`operator_key_path`].
    pub fn operator_key_path(&self) -> PathBuf {
        operator_key_path()
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

    /// Load the canonical operator key (ADR-0038), or mint it on first use.
    ///
    /// Idempotent across processes sharing one machine: the second opener loads
    /// the SAME key instead of minting a new one — the R2 "two processes share
    /// state" unit equivalent for the operator identity.
    pub fn ensure_operator_key(&self, passphrase: &str) -> anyhow::Result<crate::keys::Keys> {
        ensure_operator_key_at(
            &operator_key_path(),
            &legacy_operator_key_path(),
            passphrase,
        )
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
            home_env("XDG_CONFIG_HOME").as_deref(),
            home_env("HOME").as_deref(),
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
    /// ADR-0036 Spec 2 (bd-ewsi): state-dir resolution — XDG wins, HOME
    /// falls back, neither keeps the historical relative name. Pure (env
    /// passed in): no set_var, no ambient reads.
    #[test]
    fn state_dir_resolution_prefers_xdg_then_home() {
        let xdg = StateDirEnv {
            xdg_state_home: Some("/xdg".into()),
            home: Some("/h".into()),
        };
        assert_eq!(resolve_state_dir(&xdg), PathBuf::from("/xdg/toron"));
        let home = StateDirEnv {
            xdg_state_home: None,
            home: Some("/h".into()),
        };
        assert_eq!(
            resolve_state_dir(&home),
            PathBuf::from("/h/.local/state/toron")
        );
        assert_eq!(
            resolve_state_dir(&StateDirEnv::default()),
            PathBuf::from("toron-state")
        );
    }

    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static SEQ: AtomicU32 = AtomicU32::new(0);

    fn scratch(name: &str) -> PathBuf {
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("toron-r2-{name}-{}-{n}", std::process::id()))
    }

    fn sandbox(kind: SandboxKind) -> Option<Sandbox> {
        let temp_dir = std::env::temp_dir();
        Some(Sandbox {
            kind,
            root: temp_dir.join("toron-test-root-4242"),
            temp_dir,
        })
    }

    const KINDS: [SandboxKind; 2] = [SandboxKind::Marker, SandboxKind::TestBinary];

    #[test]
    fn env_override_beats_xdg() {
        let env = DataRootEnv {
            toron_data_dir: Some(PathBuf::from("/env/root")),
            xdg_data_home: Some(PathBuf::from("/xdg")),
            home: Some(PathBuf::from("/home")),
            sandbox: sandbox(SandboxKind::TestBinary),
        };
        assert_eq!(resolve_data_root(&env), PathBuf::from("/env/root"));
    }

    #[test]
    fn xdg_data_home_wins_over_home_fallback() {
        let env = DataRootEnv {
            toron_data_dir: None,
            xdg_data_home: Some(PathBuf::from("/xdg")),
            home: Some(PathBuf::from("/home")),
            sandbox: None,
        };
        assert_eq!(resolve_data_root(&env), PathBuf::from("/xdg/toron"));
    }

    #[test]
    fn home_fallback_is_local_share_toron() {
        let env = DataRootEnv {
            toron_data_dir: None,
            xdg_data_home: None,
            home: Some(PathBuf::from("/home/alice")),
            sandbox: None,
        };
        assert_eq!(
            resolve_data_root(&env),
            PathBuf::from("/home/alice/.local/share/toron")
        );
    }

    /// bd-fize: a test process must never resolve the operator's live root.
    ///
    /// A test that registers an agent without pinning a data dir used to
    /// write a PERMANENT identity into the operator's index — a name that can
    /// be addressed, a pubkey that can receive mail, issued by a test. The
    /// home fallback is the operator's root, so the test process takes a
    /// temp root instead.
    #[test]
    fn a_test_process_never_resolves_the_operators_live_root() {
        let env = DataRootEnv {
            toron_data_dir: None,
            xdg_data_home: None,
            home: Some(PathBuf::from("/home/alice")),
            sandbox: sandbox(SandboxKind::TestBinary),
        };
        let root = resolve_data_root(&env);
        assert_ne!(
            root,
            PathBuf::from("/home/alice/.local/share/toron"),
            "a test process must not land on the operator's live root"
        );
        assert!(
            root.starts_with(std::env::temp_dir()),
            "the test root is a temp dir: {root:?}"
        );
    }

    /// The 2026-10-24 store-skew incident, as a table. The operator's shell
    /// exports `XDG_DATA_HOME=$HOME/.local/share`, so the inherited value is
    /// the LIVE root. bd-fize honoured it before its own test check, and the
    /// spawned `toron` was not a test binary at all. Under test, both kinds
    /// refuse it and refuse the HOME fallback.
    #[test]
    fn an_ambient_xdg_data_home_is_not_a_pin_under_test() {
        for kind in KINDS {
            let sb = sandbox(kind);
            let expected = sb.as_ref().unwrap().root.clone();
            for (xdg, home) in [
                (Some("/home/alice/.local/share"), Some("/home/alice")),
                (None, Some("/home/alice")),
                (Some("/xdg"), None),
                (None, None),
            ] {
                let env = DataRootEnv {
                    toron_data_dir: None,
                    xdg_data_home: xdg.map(PathBuf::from),
                    home: home.map(PathBuf::from),
                    sandbox: sb.clone(),
                };
                assert_eq!(
                    resolve_data_root(&env),
                    expected,
                    "{kind:?} with XDG_DATA_HOME={xdg:?} HOME={home:?} must take its sandbox"
                );
            }
        }
    }

    /// A fixture home is still honoured. Tests build one under the temp root
    /// (or `/tmp`) and point `XDG_DATA_HOME` or `HOME` at it, often so that two
    /// processes share it. A `..` that climbs out is not scratch.
    #[test]
    fn a_scratch_fixture_root_is_honoured_under_test() {
        let tmp = std::env::temp_dir();
        for kind in KINDS {
            let sb = sandbox(kind);
            let fixture = tmp.join("toron-fixture-home");
            let env = DataRootEnv {
                toron_data_dir: None,
                xdg_data_home: Some(fixture.join("xdg")),
                home: Some(PathBuf::from("/home/alice")),
                sandbox: sb.clone(),
            };
            assert_eq!(resolve_data_root(&env), fixture.join("xdg").join("toron"));
            let env = DataRootEnv {
                toron_data_dir: None,
                xdg_data_home: None,
                home: Some(fixture.clone()),
                sandbox: sb.clone(),
            };
            assert_eq!(
                resolve_data_root(&env),
                fixture.join(".local").join("share").join("toron")
            );
            let env = DataRootEnv {
                toron_data_dir: None,
                xdg_data_home: Some(PathBuf::from("/tmp/ih/fixture")),
                home: None,
                sandbox: sb.clone(),
            };
            assert_eq!(
                resolve_data_root(&env),
                PathBuf::from("/tmp/ih/fixture/toron")
            );
            let env = DataRootEnv {
                toron_data_dir: None,
                xdg_data_home: Some(PathBuf::from("/tmp/../home/alice/.local/share")),
                home: None,
                sandbox: sb.clone(),
            };
            assert_eq!(resolve_data_root(&env), sb.unwrap().root);
        }
    }

    /// bd-fize: an EXPLICIT pin still wins under test. A test that must
    /// exercise a real index says so with `TORON_DATA_DIR`, and restores what
    /// it wrote.
    #[test]
    fn an_explicit_pin_still_wins_under_test() {
        for kind in KINDS {
            let env = DataRootEnv {
                toron_data_dir: Some(PathBuf::from("/env/root")),
                xdg_data_home: Some(PathBuf::from("/home/alice/.local/share")),
                home: Some(PathBuf::from("/home/alice")),
                sandbox: sandbox(kind),
            };
            assert_eq!(resolve_data_root(&env), PathBuf::from("/env/root"));
        }
    }

    /// Under test is decided by the marker (or by being a test binary), never
    /// by where the executable sits. A marker naming scratch is the sandbox;
    /// any other marker value still means "under test", in a per-process
    /// sandbox, because a marker is not a licence to pick the operator's dir.
    #[test]
    fn the_marker_decides_under_test_and_names_the_sandbox() {
        let tmp = std::env::temp_dir();
        let per_process = tmp.join("toron-test-root-77");
        let harness = tmp.join("toron-test-root-12");
        let sb = sandbox_for(Some(&harness), false, &tmp, 77).expect("marker = under test");
        assert_eq!(sb.kind, SandboxKind::Marker);
        assert_eq!(sb.root, harness, "a scratch marker names the sandbox");
        for value in ["1", "/home/alice/.local/share/toron", "/tmp/../home/alice"] {
            let sb = sandbox_for(Some(Path::new(value)), false, &tmp, 77)
                .expect("any marker value = under test");
            assert_eq!(sb.kind, SandboxKind::Marker);
            assert_eq!(sb.root, per_process, "marker {value:?} is not a root");
        }
        let sb = sandbox_for(None, true, &tmp, 77).expect("a test binary is under test");
        assert_eq!(sb.kind, SandboxKind::TestBinary);
        assert_eq!(sb.root, per_process);
        assert_eq!(
            sandbox_for(None, false, &tmp, 77),
            None,
            "no marker and not a test binary: production"
        );
    }

    /// The config, state and cache homes are sandboxed like the data root: an
    /// inherited operator `HOME` / `XDG_*` maps into the sandboxed home, a
    /// scratch fixture is honoured, an unset variable stays unset, and
    /// production is untouched.
    #[test]
    fn home_shaped_variables_map_into_the_sandboxed_home_under_test() {
        let tmp = std::env::temp_dir();
        let sb = sandbox(SandboxKind::Marker).unwrap();
        let home = sb.home();
        for (name, value, expected) in [
            ("HOME", "/home/alice", home.clone()),
            (
                "XDG_CONFIG_HOME",
                "/home/alice/.config",
                home.join(".config"),
            ),
            (
                "XDG_DATA_HOME",
                "/home/alice/.local/share",
                home.join(".local").join("share"),
            ),
            (
                "XDG_STATE_HOME",
                "/home/alice/.local/state",
                home.join(".local").join("state"),
            ),
            ("XDG_CACHE_HOME", "/home/alice/.cache", home.join(".cache")),
        ] {
            assert_eq!(
                resolve_home_var(name, Some(PathBuf::from(value)), Some(&sb)),
                Some(expected),
                "{name}={value} under test"
            );
            assert_eq!(
                resolve_home_var(name, Some(PathBuf::from(value)), None),
                Some(PathBuf::from(value)),
                "{name}={value} in production"
            );
            assert_eq!(resolve_home_var(name, None, Some(&sb)), None);
        }
        let fixture = tmp.join("toron-fixture-config");
        assert_eq!(
            resolve_home_var("XDG_CONFIG_HOME", Some(fixture.clone()), Some(&sb)),
            Some(fixture)
        );
    }

    /// In this very process (a test binary, whatever HOME the runner gave it),
    /// every ambient home-derived path is the sandbox's or a scratch fixture:
    /// the operator key, the passphrase files, the state dir (wake.sock), and
    /// flywheel's git-facts journal. Never the operator's.
    #[test]
    fn this_test_binarys_config_and_state_paths_are_sandboxed() {
        let sb = ambient_sandbox().expect("a test binary is under test");
        assert_eq!(sb.kind, SandboxKind::TestBinary);
        assert_eq!(
            sb.root,
            std::env::temp_dir().join(format!("toron-test-root-{}", std::process::id()))
        );
        let mut paths = vec![
            ("operator key", operator_key_path()),
            ("state dir", state_dir()),
            (
                "git-facts journal",
                crate::mailbox::guard::git_fact_journal_path(),
            ),
        ];
        for candidate in passphrase_file_candidates(
            home_env("XDG_CONFIG_HOME").as_deref(),
            home_env("HOME").as_deref(),
        ) {
            paths.push(("passphrase", candidate));
        }
        for (what, path) in paths {
            assert!(
                sb.is_scratch(&path),
                "the {what} must be in the sandbox or a scratch fixture, got {}",
                path.display()
            );
        }
    }

    /// bd-fize: the guard is the DIRECTORY cargo builds test binaries into,
    /// and this process IS one — so the guard is live, not theoretical.
    #[test]
    fn this_process_is_recognised_as_a_test_binary() {
        assert!(
            is_test_process(),
            "a unit-test binary lives under <target>/<profile>/deps/ — the guard \
             depends on it"
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
        assert_eq!(
            paths.operator_key_path(),
            operator_key_path(),
            "delegates to the one canonical resolution"
        );
        assert_ne!(
            paths.operator_key_path(),
            paths.root.join(OPERATOR_KEY_RELATIVE),
            "the operator key must not resolve inside the data root"
        );
        assert_eq!(paths.operator_dir(), PathBuf::from("/root/operator"));
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
        let root = scratch("opkey");
        std::fs::create_dir_all(&root).expect("dirs");
        let canonical = root.join("operator_nsec");
        let legacy = root.join("operator.ncryptsec");

        let first = ensure_operator_key_at(&canonical, &legacy, "toron").expect("first open");
        assert!(canonical.is_file(), "key file exists");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&canonical)
                .expect("meta")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "operator key is 0600");
        }

        let second = ensure_operator_key_at(&canonical, &legacy, "toron").expect("second open");
        assert_eq!(
            first.public_key(),
            second.public_key(),
            "second opener loads the SAME key (no re-mint)"
        );

        assert!(
            ensure_operator_key_at(&canonical, &legacy, "wrong").is_err(),
            "passphrase mismatch errors"
        );
        let other = scratch("opkey-legacy");
        std::fs::create_dir_all(&other).expect("dirs");
        let canonical2 = other.join("operator_nsec");
        let legacy2 = other.join("operator.ncryptsec");
        std::fs::write(&legacy2, b"legacy placeholder").expect("write legacy");
        let refused = ensure_operator_key_at(&canonical2, &legacy2, "toron");
        assert!(
            refused.is_err(),
            "a legacy-only key must refuse to mint a second identity"
        );
        assert!(
            !canonical2.exists(),
            "the refusal must not leave a freshly minted canonical key behind"
        );
        assert!(
            legacy2.is_file(),
            "the legacy key is left exactly where it was — never moved or deleted"
        );

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&other);
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
    #[allow(clippy::assertions_on_constants)]
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
    fn mailbox_root_lifts_projects_to_parent() {
        assert_eq!(
            mailbox_root(&PathBuf::from("/data/toron/projects")),
            PathBuf::from("/data/toron")
        );
        assert_eq!(
            mailbox_root(&PathBuf::from("/tmp/toron-mcp-demo-123")),
            PathBuf::from("/tmp/toron-mcp-demo-123")
        );
        assert_eq!(
            mailbox_root(&PathBuf::from("/tmp/toron-cli-plane-123")),
            PathBuf::from("/tmp/toron-cli-plane-123")
        );
    }

    #[test]
    fn passphrase_candidates_prefer_xdg_then_home() {
        let v =
            passphrase_file_candidates(Some(&PathBuf::from("/xdg")), Some(&PathBuf::from("/home")));
        assert_eq!(v[0], PathBuf::from("/xdg/toron/passphrase"));
        assert_eq!(v[1], PathBuf::from("/home/.config/toron/passphrase"));
    }

    /// ADR-0038: the operator key is a secret, so it resolves in the CONFIG
    /// home (where the passphrase already lives) and never in the data root.
    /// Pure (components passed in): no env mutation, no filesystem.
    #[test]
    fn operator_key_path_resolves_in_the_config_home_never_the_data_root() {
        assert_eq!(
            resolve_operator_key_path(Some(&PathBuf::from("/xdg")), Some(&PathBuf::from("/home"))),
            PathBuf::from("/xdg/toron/operator_nsec"),
            "XDG_CONFIG_HOME wins"
        );
        assert_eq!(
            resolve_operator_key_path(None, Some(&PathBuf::from("/home"))),
            PathBuf::from("/home/.config/toron/operator_nsec"),
            "HOME is the fallback"
        );
        assert_eq!(
            resolve_operator_key_path(None, None),
            PathBuf::from("operator_nsec"),
            "no home at all keeps the relative name"
        );
        for (xdg, home) in [
            (Some(PathBuf::from("/xdg")), Some(PathBuf::from("/home"))),
            (None, Some(PathBuf::from("/home"))),
        ] {
            let p = resolve_operator_key_path(xdg.as_deref(), home.as_deref());
            assert!(
                !p.to_string_lossy().contains("share/toron") && !p.ends_with(OPERATOR_KEY_RELATIVE),
                "the canonical key must not live under the data root: {}",
                p.display()
            );
        }
    }

    /// The mode helper must set the mode and touch NOTHING else: the key bytes
    /// ARE the identity. The implementation it replaced re-encrypted the file
    /// (a truncating write) to accomplish a chmod.
    #[cfg(unix)]
    #[test]
    fn ensure_key_mode_sets_0600_without_rewriting_the_key() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("keymode");
        std::fs::create_dir_all(&dir).expect("dirs");
        let path = dir.join("operator_nsec");
        std::fs::write(&path, b"ciphertext bytes").expect("write");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("loosen");
        let before = std::fs::read(&path).expect("read");

        ensure_key_mode_at(&path).expect("mode");

        assert_eq!(
            std::fs::read(&path).expect("re-read"),
            before,
            "a chmod must not rewrite the key — the bytes are an identity"
        );
        assert_eq!(
            std::fs::metadata(&path).expect("meta").permissions().mode() & 0o777,
            0o600,
            "the mode is the thing the function is named for"
        );
        ensure_key_mode_at(&dir.join("absent")).expect("absent is a no-op");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// ADR-0038: one operator identity per machine. A legacy data-root key is a
    /// different key, so the only states are load / mint / refuse.
    #[test]
    fn operator_key_source_never_mints_beside_a_legacy_key() {
        assert_eq!(operator_key_source(true, true), OperatorKeySource::Load);
        assert_eq!(operator_key_source(true, false), OperatorKeySource::Load);
        assert_eq!(operator_key_source(false, false), OperatorKeySource::Mint);
        assert_eq!(
            operator_key_source(false, true),
            OperatorKeySource::RefuseLegacy,
            "minting here is how a machine gets two operator identities"
        );
    }
}
