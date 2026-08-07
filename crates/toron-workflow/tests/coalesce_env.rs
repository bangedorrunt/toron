// governed-by: ADR-0004
//
// Env-dependent coalescer behavior. Integration test (own crate, not subject
// to the lib's `#![forbid(unsafe_code)]`): `std::env::set_var` / `remove_var`
// are `unsafe` in edition 2024, so these cases cannot live in the lib's unit
// tests. verify-gates only scans `src/{lib,main}.rs` roots.

use std::time::Duration;

use toron_workflow::coalesce::{DEFAULT_WINDOW, window_from_env};

/// `TORON_COALESCE_WINDOW_MS` override reads through; unset/zero/garbage
/// fall back to the default.
#[test]
fn env_window_override() {
    unsafe { std::env::set_var("TORON_COALESCE_WINDOW_MS", "123") };
    assert_eq!(window_from_env(), Duration::from_millis(123));

    unsafe { std::env::set_var("TORON_COALESCE_WINDOW_MS", "0") };
    assert_eq!(window_from_env(), DEFAULT_WINDOW);

    unsafe { std::env::set_var("TORON_COALESCE_WINDOW_MS", "garbage") };
    assert_eq!(window_from_env(), DEFAULT_WINDOW);

    unsafe { std::env::remove_var("TORON_COALESCE_WINDOW_MS") };
    assert_eq!(window_from_env(), DEFAULT_WINDOW);
}
