// governed-by: ADR-0003

use std::env;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Default bus envelope TTL in days (NIP-40 `expiration`; open Q #1 resolved
/// as recommended: global default + per-project override).
pub const DEFAULT_ENVELOPE_TTL_DAYS: u64 = 90;

/// toron configuration.
///
/// Precedence (low → high): system config → user config → project config →
/// environment → CLI flags (flags handled by clap; see `Globals`).
///
/// File paths:
///   - system: `/etc/toron/config.toml`
///   - user:   `$XDG_CONFIG_HOME/toron/config.toml` (default `~/.config/toron/config.toml`)
///   - project: `./.toron.toml` (`TORON_CONFIG` overrides the project path)
///
/// Operational defaults (ADR-0003 open Q resolved): bus envelope TTL global
/// 90 days + per-project override; federation relays empty (zero egress);
/// proof gate off (schnorr); federation hardening (AUTH/POW/allowlist) off.
/// Keyring unlock priority: macOS Keychain → Linux Secret Service →
/// passphrase file. See `examples/toron.sample.toml`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Data directory (SQLite index + Git archive root).
    pub data_dir: Option<PathBuf>,
    /// Default backend (`herdr` | `tmux`). Defaults to `herdr`.
    pub backend: Option<String>,
    /// Operator key path (ncryptsec; 0600; never the secret itself).
    pub operator_key: Option<PathBuf>,
    /// Federation relays (empty default = no network egress; opt-in).
    #[serde(default)]
    pub relays: Vec<String>,
    /// Bus envelope TTL in days (NIP-40 `expiration`; default 90).
    pub envelope_ttl_days: Option<u64>,
    /// Per-project envelope TTL override map (project slug → days).
    #[serde(default)]
    pub project_ttl_days: Vec<ProjectTtl>,
    /// Proof-gate toggle. Default off.
    pub proof_gate_enabled: Option<bool>,
    /// Trusted anchor npubs for proof-gate attestation (kind-30012).
    #[serde(default)]
    pub proof_trusted_keys: Vec<String>,
    /// Proof scheme: `schnorr` (default) | `ed25519` (legacy escape hatch).
    pub proof_scheme: Option<String>,
    /// `git2` override binary path (AM-Rust `TORON_GIT_BINARY` parity).
    pub git_binary: Option<PathBuf>,
    /// Federation hardening: NIP-42 AUTH membership gate. Default off.
    pub federation_auth: Option<bool>,
    /// Federation hardening: NIP-13 POW difficulty. Default off.
    pub federation_pow: Option<u8>,
    /// Federation hardening: relay allowlist. Default empty (reject all).
    #[serde(default)]
    pub federation_allowlist: Vec<String>,
    /// Extra MCP-over-HTTP `Host` values (Tailnet service names, reverse
    /// proxies). Loopback stays allowed; rmcp rejects anything else by
    /// default, which breaks `tailscale serve` (it forwards the Tailnet
    /// `Host`, e.g. `toron.example-tailnet.ts.net`).
    #[serde(default)]
    pub mcp_allowed_hosts: Vec<String>,
    /// `[comms]` table (ADR-0041).
    pub comms: Option<CommsConfig>,
}

/// The `[comms]` table. Unknown keys are refused, like the top level.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommsConfig {
    /// Turn the per-recipient inbox log and its verbs (`next`, `ack`, `done`,
    /// `inbox ...`) on. `TORON_COMMS_INBOX` overrides it when set. Default off.
    pub inbox_log: Option<bool>,
}

/// A per-project envelope TTL override (`{ project = "demo", days = 14 }`).
#[derive(Debug, Clone, Deserialize)]
pub struct ProjectTtl {
    pub project: String,
    pub days: u64,
}

impl Config {
    /// Load config by merging system → user → project → env.
    pub fn load(cli_config: Option<&Path>) -> Self {
        let mut cfg = Self::default();
        cfg.merge_file(&system_config_path());
        cfg.merge_file(&user_config_path());
        if let Some(path) = cli_config {
            cfg.merge_file(path);
        } else {
            cfg.merge_file(&project_config_path());
        }
        cfg.merge_env();
        cfg
    }

    /// Effective envelope TTL for a project (per-project override or global default).
    pub fn envelope_ttl_days(&self, project: Option<&str>) -> u64 {
        if let Some(slug) = project
            && let Some(pt) = self.project_ttl_days.iter().find(|pt| pt.project == slug)
        {
            return pt.days;
        }
        self.envelope_ttl_days.unwrap_or(DEFAULT_ENVELOPE_TTL_DAYS)
    }

    /// The `[comms] inbox_log` value (`None` = key absent), for
    /// `toron_core::inbox::enabled_with`.
    pub fn comms_inbox_log(&self) -> Option<bool> {
        self.comms.as_ref().and_then(|c| c.inbox_log)
    }

    /// Effective backend (defaults to `herdr`).
    pub fn effective_backend(&self) -> &str {
        self.backend.as_deref().unwrap_or("herdr")
    }

    /// Effective proof scheme (defaults to `schnorr`).
    pub fn effective_proof_scheme(&self) -> &str {
        self.proof_scheme.as_deref().unwrap_or("schnorr")
    }

    /// True if proof gate is enabled.
    pub fn proof_gate_enabled(&self) -> bool {
        self.proof_gate_enabled.unwrap_or(false)
    }

    fn merge_file(&mut self, path: &Path) {
        if !path.is_file() {
            return;
        }
        let raw = match std::fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(e) => {
                eprintln!("toron: warn: cannot read config {}: {e}", path.display());
                return;
            }
        };
        match toml::from_str::<Config>(&raw) {
            Ok(parsed) => self.merge(parsed),
            Err(e) => eprintln!("toron: warn: invalid config {}: {e}", path.display()),
        }
    }

    fn merge(&mut self, other: Self) {
        if other.data_dir.is_some() {
            self.data_dir = other.data_dir;
        }
        if other.backend.is_some() {
            self.backend = other.backend;
        }
        if other.operator_key.is_some() {
            self.operator_key = other.operator_key;
        }
        if !other.relays.is_empty() {
            self.relays = other.relays;
        }
        if other.envelope_ttl_days.is_some() {
            self.envelope_ttl_days = other.envelope_ttl_days;
        }
        if !other.project_ttl_days.is_empty() {
            self.project_ttl_days = other.project_ttl_days;
        }
        if other.proof_gate_enabled.is_some() {
            self.proof_gate_enabled = other.proof_gate_enabled;
        }
        if !other.proof_trusted_keys.is_empty() {
            self.proof_trusted_keys = other.proof_trusted_keys;
        }
        if other.proof_scheme.is_some() {
            self.proof_scheme = other.proof_scheme;
        }
        if other.git_binary.is_some() {
            self.git_binary = other.git_binary;
        }
        if other.federation_auth.is_some() {
            self.federation_auth = other.federation_auth;
        }
        if other.federation_pow.is_some() {
            self.federation_pow = other.federation_pow;
        }
        if !other.federation_allowlist.is_empty() {
            self.federation_allowlist = other.federation_allowlist;
        }
        if !other.mcp_allowed_hosts.is_empty() {
            self.mcp_allowed_hosts = other.mcp_allowed_hosts;
        }
        // Key by key, so a project file that sets nothing in `[comms]` does
        // not erase what the user file set.
        if let Some(c) = other.comms
            && c.inbox_log.is_some()
        {
            self.comms
                .get_or_insert_with(CommsConfig::default)
                .inbox_log = c.inbox_log;
        }
    }

    fn merge_env(&mut self) {
        // No secrets in env values — only paths/booleans (ADR CLI design).
        if let Ok(v) = env::var("TORON_CONFIG")
            && !v.is_empty()
        {
            self.merge_file(Path::new(&v));
        }
        if let Ok(v) = env::var("TORON_DATA_DIR")
            && !v.is_empty()
        {
            self.data_dir = Some(PathBuf::from(v));
        }
        if let Ok(v) = env::var("TORON_BACKEND")
            && !v.is_empty()
        {
            self.backend = Some(v);
        }
        if let Ok(v) = env::var("TORON_OPERATOR_KEY")
            && !v.is_empty()
        {
            self.operator_key = Some(PathBuf::from(v));
        }
        if let Ok(v) = env::var("TORON_ENVELOPE_TTL_DAYS")
            && let Ok(days) = v.parse::<u64>()
        {
            self.envelope_ttl_days = Some(days);
        }
        if let Ok(v) = env::var("TORON_PROOF_GATE_ENABLED")
            && let Ok(b) = v.parse::<bool>()
        {
            self.proof_gate_enabled = Some(b);
        }
        if let Ok(v) = env::var("TORON_PROOF_SCHEME")
            && !v.is_empty()
        {
            self.proof_scheme = Some(v);
        }
        if let Ok(v) = env::var("TORON_MCP_ALLOWED_HOSTS")
            && !v.is_empty()
        {
            self.mcp_allowed_hosts = v
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }
    }
}

fn system_config_path() -> PathBuf {
    PathBuf::from("/etc/toron/config.toml")
}

fn user_config_path() -> PathBuf {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return PathBuf::from(xdg).join("toron").join("config.toml");
    }
    if let Some(home) = env::var_os("HOME") {
        return PathBuf::from(home)
            .join(".config")
            .join("toron")
            .join("config.toml");
    }
    PathBuf::from(".toron.toml")
}

fn project_config_path() -> PathBuf {
    PathBuf::from(".toron.toml")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    static COUNTER: AtomicU32 = AtomicU32::new(0);

    fn scratch(name: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "toron-config-test-{}-{}-{name}",
            std::process::id(),
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_config(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    #[test]
    fn project_config_overrides_system_defaults() {
        let dir = scratch("project");
        let cfg_path = write_config(&dir, ".toron.toml", "backend = \"herdr\"\n");
        let cfg = Config::load(Some(&cfg_path));
        assert_eq!(cfg.backend.as_deref(), Some("herdr"));
    }

    #[test]
    fn explicit_config_flag_beats_project_and_defaults() {
        let dir = scratch("flag");
        let _project = write_config(&dir, ".toron.toml", "backend = \"tmux\"\n");
        let flag = write_config(&dir, "override.toml", "backend = \"herdr\"\n");
        let cfg = Config::load(Some(&flag));
        assert_eq!(cfg.backend.as_deref(), Some("herdr"));
    }

    #[test]
    fn per_project_ttl_overrides_global() {
        let dir = scratch("ttl");
        let cfg_path = write_config(
            &dir,
            ".toron.toml",
            "envelope_ttl_days = 90\n[[project_ttl_days]]\nproject = \"demo\"\ndays = 14\n",
        );
        let cfg = Config::load(Some(&cfg_path));
        assert_eq!(cfg.envelope_ttl_days(None), 90);
        assert_eq!(cfg.envelope_ttl_days(Some("demo")), 14);
        assert_eq!(cfg.envelope_ttl_days(Some("other")), 90);
    }

    #[test]
    fn mcp_allowed_hosts_merge_and_env() {
        // File value merges.
        let dir = scratch("mcp-hosts");
        let cfg_path = write_config(
            &dir,
            ".toron.toml",
            "mcp_allowed_hosts = [\"toron.example-tailnet.ts.net\"]\n",
        );
        let cfg = Config::load(Some(&cfg_path));
        assert_eq!(cfg.mcp_allowed_hosts, vec!["toron.example-tailnet.ts.net"]);
        // Default is empty (loopback only).
        let cfg = Config::default();
        assert!(cfg.mcp_allowed_hosts.is_empty());
    }

    #[test]
    fn comms_inbox_log_parses_and_defaults_to_absent() {
        let dir = scratch("comms");
        let on = write_config(&dir, "on.toml", "[comms]\ninbox_log = true\n");
        assert_eq!(Config::load(Some(&on)).comms_inbox_log(), Some(true));
        let off = write_config(&dir, "off.toml", "[comms]\ninbox_log = false\n");
        assert_eq!(Config::load(Some(&off)).comms_inbox_log(), Some(false));
        // CONTROL: a config with no `[comms]` leaves the key absent.
        let none = write_config(&dir, "none.toml", "backend = \"herdr\"\n");
        assert_eq!(Config::load(Some(&none)).comms_inbox_log(), None);
    }

    #[test]
    fn an_unknown_comms_key_is_refused_like_any_other() {
        let err = toml::from_str::<Config>("[comms]\ninbox_logg = true\n").unwrap_err();
        assert!(err.to_string().contains("inbox_logg"), "{err}");
    }

    #[test]
    fn comms_merges_key_by_key_so_a_silent_layer_does_not_erase_it() {
        let mut cfg = Config::default();
        cfg.merge(toml::from_str("[comms]\ninbox_log = true\n").unwrap());
        // A later layer that sets other things, or an empty `[comms]`, keeps it.
        cfg.merge(toml::from_str("backend = \"tmux\"\n").unwrap());
        cfg.merge(toml::from_str("[comms]\n").unwrap());
        assert_eq!(cfg.comms_inbox_log(), Some(true));
        // A later layer that sets it wins.
        cfg.merge(toml::from_str("[comms]\ninbox_log = false\n").unwrap());
        assert_eq!(cfg.comms_inbox_log(), Some(false));
    }

    #[test]
    fn the_sample_config_documents_the_comms_table() {
        let sample = include_str!("../../../examples/toron.sample.toml");
        let uncommented =
            sample.replace("# [comms]\n# inbox_log = true", "[comms]\ninbox_log = true");
        let cfg: Config = toml::from_str(&uncommented).expect("sample parses once enabled");
        assert_eq!(cfg.comms_inbox_log(), Some(true));
    }
}
