// governed-by: ADR-0005 (P4.7 — P4 Behavior Test Matrix harness fixtures)
#![forbid(unsafe_code)]
// F20.DEADCODE-FIXTURES (batch-fix): test-support module — the fixtures
// (housekeeper_workflow, cron_registry, next_tick, fixture_registration) are
// referenced by p1-gated scenario files compiled out in some feature combos;
// the lint firing on a test-support module is noise, not dead code.
#![allow(dead_code)]

//! Shared fixtures for the scenario test harness (bd-i84w: the vendor
//! server harness is retired — `boot_server` / `wait_ready` / `free_port` /
//! vendor helpers are gone with it; scenarios drive `DurableRun` /
//! `DurableGraphRun` over temp sqlite pools via [`durable`]).
//!
//! Usage: each scenario file does `#[path = "common/mod.rs"] mod common;`
//! and calls into it. These are FIXTURES, not assertions — the scenario
//! files own their behavior checks.

pub mod durable;

use std::sync::Arc;

use toron_workflow::engine::{ActionInvocation, AsyncEffectRunner, ProfileResolver};
use toron_workflow::schema::Action;

/// A no-op async effect runner (steps succeed, return null). Pure, no I/O.
pub fn noop_runner() -> AsyncEffectRunner {
    Arc::new(|_: &ActionInvocation| {
        Box::pin(async move { Ok(serde_json::Value::Null) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

/// Lift a PURE leaf-value fn into the async seam (fixtures only): no I/O,
/// no blocking — the async wrapper around pure computation. Closures that
/// perform I/O belong in fenced host runners, never here.
pub fn lift_async(
    f: impl Fn(&Action) -> anyhow::Result<serde_json::Value> + Send + Sync + 'static,
) -> AsyncEffectRunner {
    Arc::new(move |inv: &ActionInvocation| {
        let value = f(&inv.action);
        Box::pin(async move { value })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

/// Resolves `@housekeeper` to a spawn spec (the canonical P4 owner).
pub fn housekeeper_resolver() -> ProfileResolver {
    Arc::new(|name: &str| {
        if name == "@housekeeper" {
            Some(serde_json::json!({
                "npub": "npub1housekeeper",
                "spec": { "harness": "pi", "model": "claude-sonnet-4", "backend": "hc" },
            }))
        } else {
            None
        }
    })
}

/// Path to the `toron` CLI binary that the CLI scenarios drive.
///
/// `CARGO_BIN_EXE_toron` is only set for binaries belonging to THIS package,
/// and this package has none, so the location has to be derived from the test
/// executable itself.
///
/// The previous fallback assumed cargo's `<target>/<profile>/deps/` layout and
/// walked exactly two levels up from the test binary. Build wrappers do not
/// honour that layout: under `mbx` a test binary lands in
/// `<target>/<profile>/build/<pkg>/<hash>/deps/`, so the two-level walk
/// resolved to `<target>/<profile>/build/<pkg>/<hash>/toron`, which never
/// exists, and eight scenario tests failed on infrastructure alone.
///
/// Walking ancestors and taking the first directory that genuinely contains a
/// `toron` file works under both layouts, because the test binary always sits
/// somewhere beneath the profile directory. It also does not care how deep the
/// wrapper nested it, so another layer cannot silently re-break this.
///
/// Panics naming the build command when the binary is genuinely absent, which
/// is the one case where the test binary genuinely cannot run.
pub fn toron_bin() -> std::path::PathBuf {
    // spawn-harness: locates the binary only; every spawn wraps it in
    // `toron_core::testkit::toron_command`.
    if let Ok(p) = std::env::var("CARGO_BIN_EXE_toron") {
        return std::path::PathBuf::from(p);
    }
    let exe = std::env::current_exe().expect("current_exe");
    for dir in exe.ancestors() {
        let candidate = dir.join("toron");
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!(
        "toron binary not found in any ancestor of {} — build it first \
         (cargo build -p toron-cli)",
        exe.display()
    );
}
