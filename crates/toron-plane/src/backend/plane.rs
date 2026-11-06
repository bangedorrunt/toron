// governed-by: ADR-0045

//! Pane-plane resolution: rex first, hc as the transport fallback.
//!
//! This module is the ONE place that decides which multiplexer plane serves.
//! [`resolve`] is a **pure function of an [`PlaneEnv`] snapshot**; the
//! filesystem/PATH probe happens once, at the edge, in [`RexCli::detect`]. The
//! decision table is therefore testable without spawning either backend, and no
//! domain code reads the clock, the environment, or the filesystem.
//!
//! [`connect`] is the impure shell: it builds the backend the resolved plane
//! names, and is the only construction site callers should use.
//!
//! Two honesty rules drive the table:
//!
//! 1. **`auto` never builds a primary it already knows is absent.** A missing
//!    rex CLI resolves straight to hc with the reason, instead of paying a
//!    failed spawn on every call and falling back invisibly.
//! 2. **An explicit pin never falls back.** `rex` without a CLI is an error at
//!    construction, naming the missing CLI — not a quiet ride on hc.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::OnceLock;

use super::Backend;
use super::error::{BackendError, BackendErrorCode};
use super::fallback::FallbackBackend;
use super::hc::HcBackend;
use super::superlogical::SuperlogicalBackend;

/// The default plane when nothing is configured: rex first, hc otherwise.
pub const DEFAULT_PLANE: PlaneChoice = PlaneChoice::Auto;

/// What the environment or config asked for, parsed once at the boundary.
///
/// Parsing is total: an unrecognized name is [`PlaneChoice::Unknown`], carried
/// verbatim so the error can quote what the operator actually typed. Downstream
/// code never compares raw strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlaneChoice {
    /// rex when the CLI answers, hc otherwise.
    Auto,
    /// rex, or a loud failure. Never falls back.
    Rex,
    /// Alias of [`PlaneChoice::Rex`], kept because flywheel spells it this way.
    Superlogical,
    /// hc, chosen on purpose. Not a fallback.
    Hc,
    /// Not a backend name. [`connect`] refuses it.
    Unknown(String),
}

impl PlaneChoice {
    /// Parse a configured name. An empty string means "unset" and selects the
    /// default; matching is ASCII-case-insensitive and trims surrounding space.
    pub fn parse(raw: &str) -> Self {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return DEFAULT_PLANE;
        }
        match trimmed.to_ascii_lowercase().as_str() {
            "auto" => Self::Auto,
            "rex" => Self::Rex,
            "superlogical" => Self::Superlogical,
            "hc" => Self::Hc,
            _ => Self::Unknown(trimmed.to_string()),
        }
    }

    /// The spelling an operator should use (the canonical value, never the
    /// alias).
    pub fn as_str(&self) -> &str {
        match self {
            Self::Auto => "auto",
            Self::Rex => "rex",
            Self::Superlogical => "superlogical",
            Self::Hc => "hc",
            Self::Unknown(raw) => raw,
        }
    }

    /// Whether this choice is a deliberate rex pin, which must never fall back.
    pub fn is_explicit_rex(&self) -> bool {
        matches!(self, Self::Rex | Self::Superlogical)
    }
}

impl fmt::Display for PlaneChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for PlaneChoice {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self::parse(s))
    }
}

/// The result of the rex CLI presence probe.
///
/// Presence only: `rex` is never executed here. `rex whoami`/`rex ls` would
/// autostart a server (`--autostart` defaults to true), which a status probe
/// must never do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RexCli {
    Found(PathBuf),
    Missing(String),
}

impl RexCli {
    /// Probe for an executable `rex`.
    ///
    /// When `rex_bin_dir` is set, only that directory is considered — the same
    /// contract as `Rex::resolve`, which does not fall through to PATH. This
    /// function touches the filesystem, so it is an edge effect: pass the
    /// snapshot into [`resolve`] rather than probing inside it.
    pub fn detect(rex_bin_dir: Option<&Path>) -> Self {
        if let Some(dir) = rex_bin_dir {
            let path = dir.join("rex");
            return if executable_file(&path) {
                Self::Found(path)
            } else {
                Self::Missing(format!(
                    "rex CLI not found ({} is not an executable file)",
                    path.display()
                ))
            };
        }
        let Some(paths) = std::env::var_os("PATH") else {
            return Self::Missing("rex CLI not found (PATH is not set)".to_string());
        };
        std::env::split_paths(&paths)
            .map(|dir| dir.join("rex"))
            .find(|path| executable_file(path))
            .map_or_else(
                || Self::Missing("rex CLI not found (no executable rex on PATH)".to_string()),
                Self::Found,
            )
    }

    /// Probe using the process environment (`REX_BIN_DIR`, then PATH).
    pub fn detect_env() -> Self {
        let dir = std::env::var_os("REX_BIN_DIR")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        Self::detect(dir.as_deref())
    }

    pub fn is_present(&self) -> bool {
        matches!(self, Self::Found(_))
    }

    /// Why the CLI is absent, or an empty string when it was found.
    pub fn reason(&self) -> &str {
        match self {
            Self::Found(_) => "",
            Self::Missing(reason) => reason,
        }
    }

    fn path(&self) -> Option<&Path> {
        match self {
            Self::Found(path) => Some(path),
            Self::Missing(_) => None,
        }
    }
}

fn executable_file(path: &Path) -> bool {
    if !path.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(path)
            .map(|meta| meta.permissions().mode() & 0o111 != 0)
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// The inputs the plane decision depends on. Construct it at an edge and pass
/// it down; [`resolve`] never reads anything itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaneEnv {
    pub choice: PlaneChoice,
    pub rex_cli: RexCli,
    /// An explicit hc socket. `None` means the default resolution
    /// (`HC_SOCKET_PATH`, then the XDG state dir). An embedder that owns a
    /// configurable socket (flywheel's `[backend] socket_path`) passes it here
    /// instead of forking the decision table.
    pub hc_socket: Option<String>,
}

impl PlaneEnv {
    pub fn new(choice: PlaneChoice, rex_cli: RexCli) -> Self {
        Self {
            choice,
            rex_cli,
            hc_socket: None,
        }
    }

    /// Override the hc socket. Data, not an effect: [`resolve`] ignores it and
    /// [`connect`] passes it to the hc client.
    pub fn with_hc_socket(mut self, socket: Option<String>) -> Self {
        self.hc_socket = socket;
        self
    }

    /// Parse `raw` against an explicit probe result (race-free for tests and
    /// for callers that already hold a probe).
    pub fn detect(raw: &str, rex_bin_dir: Option<&Path>) -> Self {
        Self {
            choice: PlaneChoice::parse(raw),
            rex_cli: RexCli::detect(rex_bin_dir),
            hc_socket: None,
        }
    }

    /// Parse `raw` against the process environment. Edge only.
    pub fn from_raw(raw: &str) -> Self {
        Self {
            choice: PlaneChoice::parse(raw),
            rex_cli: RexCli::detect_env(),
            hc_socket: None,
        }
    }
}

/// The plane that will actually serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Plane {
    Rex,
    Hc,
    /// The configured name is not a backend. [`connect`] refuses it.
    Invalid,
}

impl Plane {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Rex => "rex",
            Self::Hc => "hc",
            Self::Invalid => "none",
        }
    }
}

impl fmt::Display for Plane {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Which plane will serve, and why. ONE derivation for every consumer, because
/// the configured name is not evidence that the plane can answer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ResolvedPlane {
    /// What the env/config asked for.
    pub configured: PlaneChoice,
    /// What will serve.
    pub plane: Plane,
    /// The rex CLI answered the presence probe.
    pub rex_reachable: bool,
    /// One sentence: which layer chose this plane, and why.
    pub reason: String,
}

impl ResolvedPlane {
    /// `auto` that had to leave the primary for hc. An explicit `hc` is a
    /// choice, not a fallback.
    pub fn is_fallback(&self) -> bool {
        self.plane == Plane::Hc && self.configured == PlaneChoice::Auto
    }

    pub fn serving(&self) -> &'static str {
        self.plane.name()
    }
}

/// Decide the serving plane. Pure.
///
/// | choice | rex CLI | plane | fallback? |
/// |---|---|---|---|
/// | `hc` | any | hc | no (configured) |
/// | `rex` / `superlogical` | found | rex | no (a pin) |
/// | `rex` / `superlogical` | missing | rex, unreachable | no — [`connect`] fails loud |
/// | `auto` | found | rex | primary, hc on disconnect |
/// | `auto` | missing | hc | yes, with the reason |
/// | unknown | any | invalid | no — [`connect`] refuses |
pub fn resolve(env: &PlaneEnv) -> ResolvedPlane {
    let configured = env.choice.clone();
    let (plane, rex_reachable, reason) = match &env.choice {
        PlaneChoice::Hc => (Plane::Hc, false, "hc (configured)".to_string()),
        choice @ (PlaneChoice::Rex | PlaneChoice::Superlogical) => match env.rex_cli.path() {
            Some(path) => (
                Plane::Rex,
                true,
                format!("{}: explicit rex pin ({})", choice, path.display()),
            ),
            None => (
                Plane::Rex,
                false,
                format!("{}: explicit rex pin, but {}", choice, env.rex_cli.reason()),
            ),
        },
        PlaneChoice::Auto => match env.rex_cli.path() {
            Some(path) => (
                Plane::Rex,
                true,
                format!("auto -> rex ({})", path.display()),
            ),
            None => (
                Plane::Hc,
                false,
                format!("auto -> hc fallback: {}", env.rex_cli.reason()),
            ),
        },
        PlaneChoice::Unknown(raw) => (
            Plane::Invalid,
            false,
            format!("{raw}: unknown multiplexer backend (want auto, rex, or hc)"),
        ),
    };
    ResolvedPlane {
        configured,
        plane,
        rex_reachable,
        reason,
    }
}

/// Resolve using the process environment (`TORON_BACKEND`, `REX_BIN_DIR`, PATH).
pub fn resolve_default() -> ResolvedPlane {
    resolve(&PlaneEnv::from_raw(&configured_name()))
}

fn configured_name() -> String {
    std::env::var("TORON_BACKEND").unwrap_or_default()
}

/// Build the backend the resolved plane names. The one construction site.
///
/// # Errors
///
/// - The configured name is not a backend (`Unsupported`).
/// - An explicit rex pin cannot find the rex CLI (`Io`), naming the path.
/// - The resolved hc plane cannot reach its socket (`Connect`).
pub fn connect(env: &PlaneEnv) -> Result<Box<dyn Backend>, BackendError> {
    let resolved = resolve(env);
    match resolved.plane {
        Plane::Invalid => Err(BackendError::new(
            BackendErrorCode::Unsupported,
            resolved.reason,
        )),
        Plane::Hc => {
            if resolved.is_fallback() {
                note_fallback_once(&resolved.reason);
            }
            Ok(Box::new(HcBackend::connect(env.hc_socket.as_deref())?))
        }
        Plane::Rex => {
            if !resolved.rex_reachable {
                return Err(BackendError::new(BackendErrorCode::Io, resolved.reason));
            }
            let primary: Box<dyn Backend> = Box::new(SuperlogicalBackend);
            if env.choice.is_explicit_rex() {
                // A pin is a pin: a transport failure surfaces, never rides hc.
                return Ok(primary);
            }
            let fallback: Option<Box<dyn Backend>> =
                match HcBackend::connect(env.hc_socket.as_deref()) {
                    Ok(hc) => Some(Box::new(hc)),
                    Err(error) => {
                        tracing::debug!(%error, "auto: hc fallback unavailable; rex only");
                        None
                    }
                };
            Ok(Box::new(FallbackBackend::new(primary, fallback)))
        }
    }
}

/// [`connect`] using the process environment. Edge only: the mailbox and MCP
/// read paths call this because they hold no `Config`.
pub fn connect_default() -> Result<Box<dyn Backend>, BackendError> {
    connect(&PlaneEnv::from_raw(&configured_name()))
}

/// One line per process when `auto` could not stay on rex. The fallback is
/// per call (that is the transport contract); the operator hears about it once.
fn note_fallback_once(reason: &str) {
    static ONCE: OnceLock<()> = OnceLock::new();
    if ONCE.set(()).is_ok() {
        eprintln!("toron: pane plane: {reason} — every pane call in this process rides it");
    }
}

#[cfg(test)]
mod plane_tests {
    use super::*;

    fn env(choice: &str, cli: RexCli) -> PlaneEnv {
        PlaneEnv::new(PlaneChoice::parse(choice), cli)
    }

    fn found() -> RexCli {
        RexCli::Found(PathBuf::from("/opt/rex/bin/rex"))
    }

    fn missing() -> RexCli {
        RexCli::Missing("rex CLI not found (no executable rex on PATH)".to_string())
    }

    /// Mutation: default to hc and this fails — `auto` is the configured
    /// default, and rex serves it when the CLI is present.
    #[test]
    fn auto_with_a_rex_cli_resolves_to_rex() {
        let resolved = resolve(&env("auto", found()));
        assert_eq!(resolved.configured, PlaneChoice::Auto);
        assert_eq!(resolved.plane, Plane::Rex);
        assert!(resolved.rex_reachable);
        assert!(!resolved.is_fallback());
        assert!(
            resolved.reason.starts_with("auto -> rex ("),
            "{}",
            resolved.reason
        );
    }

    /// Mutation: build a rex primary anyway (flywheel's old shape) and the
    /// plane says rex while every call pays a failed spawn. The measured
    /// failure was an hc terminal set built, then `rex run` dying.
    #[test]
    fn auto_without_a_rex_cli_resolves_to_hc_with_the_reason() {
        let resolved = resolve(&env("auto", missing()));
        assert_eq!(resolved.plane, Plane::Hc);
        assert!(!resolved.rex_reachable);
        assert!(resolved.is_fallback());
        assert!(
            resolved
                .reason
                .starts_with("auto -> hc fallback: rex CLI not found"),
            "{}",
            resolved.reason
        );
    }

    /// An explicit pin keeps the rex plane even with no CLI, and is NOT a
    /// fallback: `connect` fails loud instead of building an hc backend.
    #[test]
    fn an_explicit_rex_pin_never_reports_a_fallback() {
        for name in ["rex", "superlogical"] {
            let resolved = resolve(&env(name, missing()));
            assert_eq!(resolved.plane, Plane::Rex, "{name}");
            assert!(!resolved.rex_reachable);
            assert!(!resolved.is_fallback(), "{name} is a pin, not a fallback");
            assert!(
                resolved
                    .reason
                    .contains("explicit rex pin, but rex CLI not found"),
                "{}",
                resolved.reason
            );
        }
    }

    /// `hc` is a choice, not a fallback: no probe result can move it.
    #[test]
    fn explicit_hc_never_reports_a_fallback() {
        let resolved = resolve(&env("hc", found()));
        assert_eq!(resolved.plane, Plane::Hc);
        assert!(!resolved.rex_reachable);
        assert!(!resolved.is_fallback());
        assert_eq!(resolved.reason, "hc (configured)");
    }

    /// A typo must not become a silent hc ride.
    #[test]
    fn an_unknown_name_resolves_to_the_invalid_plane() {
        let resolved = resolve(&env("nonsense", found()));
        assert_eq!(resolved.plane, Plane::Invalid);
        assert_eq!(resolved.configured, PlaneChoice::Unknown("nonsense".into()));
        assert!(
            resolved.reason.contains("want auto, rex, or hc"),
            "{}",
            resolved.reason
        );
    }

    /// Parsing is total and case-insensitive; the empty string is the default.
    #[test]
    fn choice_parsing_is_total() {
        assert_eq!(PlaneChoice::parse(""), DEFAULT_PLANE);
        assert_eq!(PlaneChoice::parse("  AUTO "), PlaneChoice::Auto);
        assert_eq!(PlaneChoice::parse("HC"), PlaneChoice::Hc);
        assert_eq!(PlaneChoice::parse("Rex"), PlaneChoice::Rex);
        assert_eq!(
            PlaneChoice::parse("tmux"),
            PlaneChoice::Unknown("tmux".into())
        );
        assert_eq!("auto".parse::<PlaneChoice>(), Ok(PlaneChoice::Auto));
        assert_eq!(PlaneChoice::Unknown("tmux".into()).as_str(), "tmux");
        assert!(PlaneChoice::Rex.is_explicit_rex());
        assert!(PlaneChoice::Superlogical.is_explicit_rex());
        assert!(!PlaneChoice::Auto.is_explicit_rex());
    }

    /// The probe is presence-only and honors `REX_BIN_DIR` with no PATH
    /// fall-through — the same contract as `Rex::resolve`.
    #[test]
    fn the_probe_reads_only_the_given_directory() {
        let empty = tempfile::tempdir().expect("empty rex dir");
        assert!(!RexCli::detect(Some(empty.path())).is_present());

        let bin = tempfile::tempdir().expect("rex dir");
        let rex = bin.path().join("rex");
        std::fs::write(&rex, b"#!/bin/sh\nexit 0\n").expect("write rex");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&rex, std::fs::Permissions::from_mode(0o755))
                .expect("chmod rex");
        }
        let probed = RexCli::detect(Some(bin.path()));
        assert!(probed.is_present(), "{probed:?}");
        assert_eq!(probed.path(), Some(rex.as_path()));
        assert_eq!(probed.reason(), "");
    }

    /// flywheel owns a configurable hc socket; it rides the env snapshot
    /// instead of forking the decision table. `resolve` must ignore it.
    #[test]
    fn the_plane_env_carries_an_explicit_hc_socket() {
        let env = PlaneEnv::new(PlaneChoice::Hc, missing())
            .with_hc_socket(Some("/tmp/flywheel-hc.sock".to_string()));
        assert_eq!(env.hc_socket.as_deref(), Some("/tmp/flywheel-hc.sock"));
        assert_eq!(resolve(&env).plane, Plane::Hc);
        assert_eq!(resolve(&env).reason, "hc (configured)");
    }

    /// A directory named `rex` is not an executable file.
    #[test]
    fn a_directory_is_not_a_rex_cli() {
        let dir = tempfile::tempdir().expect("rex dir");
        std::fs::create_dir(dir.path().join("rex")).expect("mkdir rex");
        assert!(!RexCli::detect(Some(dir.path())).is_present());
    }
}
