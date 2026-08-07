// governed-by: ADR-0004
//
// Env-dependent guard-evaluator behavior. Integration test (own crate, not
// subject to the lib's `#![forbid(unsafe_code)]`): `std::env::set_var` /
// `remove_var` are `unsafe` in edition 2024, so these cases cannot live in
// the lib's unit tests. verify-gates only scans `src/{lib,main}.rs` roots.

use std::collections::HashMap;
use std::time::Duration;

use toron_workflow::{DEFAULT_TIMEOUT_MS, evaluate_guard_default, timeout_from_env};

/// `TORON_GUARD_TIMEOUT_MS` override reads through; unset falls back.
#[test]
fn env_override_reads_through() {
    // Precondition: whatever the ambient env, force the override.
    unsafe { std::env::set_var("TORON_GUARD_TIMEOUT_MS", "42") };
    assert_eq!(timeout_from_env(), Duration::from_millis(42));

    unsafe { std::env::set_var("TORON_GUARD_TIMEOUT_MS", "0") };
    assert_eq!(
        timeout_from_env(),
        Duration::from_millis(DEFAULT_TIMEOUT_MS)
    );

    unsafe { std::env::set_var("TORON_GUARD_TIMEOUT_MS", "garbage") };
    assert_eq!(
        timeout_from_env(),
        Duration::from_millis(DEFAULT_TIMEOUT_MS)
    );

    unsafe { std::env::remove_var("TORON_GUARD_TIMEOUT_MS") };
    assert_eq!(
        timeout_from_env(),
        Duration::from_millis(DEFAULT_TIMEOUT_MS)
    );
}

/// `evaluate_guard_default` uses the env timeout.
#[tokio::test]
async fn default_uses_env_timeout() {
    unsafe { std::env::set_var("TORON_GUARD_TIMEOUT_MS", "200") };
    let out = evaluate_guard_default(Some("true"), &HashMap::new()).await;
    assert_eq!(out, toron_workflow::GuardOutcome::Bool(true));
    unsafe { std::env::remove_var("TORON_GUARD_TIMEOUT_MS") };
}
