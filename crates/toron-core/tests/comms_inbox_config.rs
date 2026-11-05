// governed-by: ADR-0041
//! The comms flag decision (bd-652a).
//!
//! `TORON_COMMS_INBOX` was the only switch for the inbox verbs; ADR-0041
//! deferred a `[comms] inbox_log` config key until the loader had a `[comms]`
//! table. The decision is one pure function, `resolve_enabled(env, config)`,
//! so the whole table is pinned here without touching the process
//! environment. The CLI half (the key is read from real config files and the
//! verbs honour it) is in toron-cli's `comms_flag_config` test.

use toron_core::inbox::resolve_enabled;

/// REPRO: before this, config could not turn the verbs on at all. Env unset
/// and `inbox_log = true` is on.
#[test]
fn config_turns_the_log_on_when_the_env_var_is_unset() {
    assert!(resolve_enabled(None, Some(true)));
    assert!(
        resolve_enabled(Some(""), Some(true)),
        "empty counts as unset"
    );
}

/// The env var wins whenever it is set: it can force on over a false/absent
/// config, and force off over a true config.
#[test]
fn the_env_var_overrides_config_both_ways() {
    for on in ["1", "true", "on"] {
        assert!(resolve_enabled(Some(on), None), "{on}");
        assert!(resolve_enabled(Some(on), Some(false)), "{on} over false");
    }
    for off in ["0", "false", "off"] {
        assert!(!resolve_enabled(Some(off), Some(true)), "{off} over true");
        assert!(!resolve_enabled(Some(off), None), "{off}");
    }
}

/// An env value nobody recognises is off, even over `inbox_log = true`: a
/// typo must never switch a durable log on.
#[test]
fn an_unrecognised_env_value_fails_closed() {
    for junk in ["yes", "ON", "True", "2", " 1", "enabled"] {
        assert!(!resolve_enabled(Some(junk), Some(true)), "{junk:?}");
        assert!(!resolve_enabled(Some(junk), None), "{junk:?}");
    }
}

/// CONTROL: nothing set anywhere is on. An explicit false config still opts out.
#[test]
fn control_default_is_on() {
    assert!(resolve_enabled(None, None));
    assert!(resolve_enabled(Some(""), None));
}

/// CONTROL: `inbox_log = false` is off, and the env var alone still turns it
/// on exactly as it did before the key existed.
#[test]
fn control_config_false_is_off_and_env_alone_still_works() {
    assert!(!resolve_enabled(None, Some(false)));
    assert!(resolve_enabled(Some("1"), None));
}
