// governed-by: ADR-0004

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
