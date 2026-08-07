// governed-by: ADR-0003
//
// Env-dependent `TORON_GIT_BINARY` override behavior. Integration test (own
// crate, not subject to the lib's `#![forbid(unsafe_code)]`): `set_var` /
// `remove_var` are `unsafe` in edition 2024, so this cannot live in the lib's
// unit tests. verify-gates only scans `src/{lib,main}.rs` roots.

use toron_core::mailbox::guard::git_binary_override;

/// Unset → None (libgit2 default engine); set → the path override.
#[test]
fn git_binary_override_env() {
    unsafe { std::env::remove_var("TORON_GIT_BINARY") };
    assert!(git_binary_override().is_none(), "unset → None");

    unsafe { std::env::set_var("TORON_GIT_BINARY", "/usr/bin/git") };
    assert_eq!(
        git_binary_override().map(|p| p.to_string_lossy().into_owned()),
        Some("/usr/bin/git".to_string())
    );

    unsafe { std::env::remove_var("TORON_GIT_BINARY") };
}
