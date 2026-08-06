// governed-by: ADR-0003

use std::env;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// toron configuration.
///
/// Precedence (low → high): system config → user config → project config →
/// environment → CLI flags (flags handled by clap; see `Globals`).
///
/// File paths:
///   - system: `/etc/toron/config.toml`
///   - user:   `$XDG_CONFIG_HOME/toron/config.toml` (default `~/.config/toron/config.toml`)
///   - project: `./.toron.toml`
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Data directory (SQLite index + Git archive root).
    pub data_dir: Option<PathBuf>,
    /// Default backend (`herdr` | `tmux`).
    pub backend: Option<String>,
    /// Operator key path (ncryptsec; never the secret itself).
    pub operator_key: Option<PathBuf>,
    /// Federation relays (empty default = no network egress; opt-in).
    #[serde(default)]
    pub relays: Vec<String>,
    /// Bus envelope TTL in days (NIP-40 `expiration`; default 90).
    pub envelope_ttl_days: Option<u64>,
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
    }

    fn merge_env(&mut self) {
        if let Ok(v) = env::var("TORON_DATA_DIR") {
            if !v.is_empty() {
                self.data_dir = Some(PathBuf::from(v));
            }
        }
        if let Ok(v) = env::var("TORON_BACKEND") {
            if !v.is_empty() {
                self.backend = Some(v);
            }
        }
        if let Ok(v) = env::var("TORON_OPERATOR_KEY") {
            if !v.is_empty() {
                self.operator_key = Some(PathBuf::from(v));
            }
        }
        if let Ok(v) = env::var("TORON_ENVELOPE_TTL_DAYS") {
            if let Ok(days) = v.parse::<u64>() {
                self.envelope_ttl_days = Some(days);
            }
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
        // A --config flag replaces the project file in the chain.
        let cfg = Config::load(Some(&flag));
        assert_eq!(cfg.backend.as_deref(), Some("herdr"));
    }

    #[test]
    fn relays_default_to_empty() {
        let cfg = Config::load(None);
        assert!(cfg.relays.is_empty(), "federation must be opt-in");
    }
}
