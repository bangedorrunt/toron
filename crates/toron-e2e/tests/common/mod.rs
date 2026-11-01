// governed-by: ADR-0005 (P4.7 — P4 Behavior Test Matrix harness fixtures)
#![forbid(unsafe_code)]
// F20.DEADCODE-FIXTURES (batch-fix): test-support module — the fixtures
// (housekeeper_workflow, cron_registry, next_tick, fixture_registration) are
// referenced by p1-gated scenario files compiled out in some feature combos;
// the lint firing on a test-support module is noise, not dead code.
#![allow(dead_code)]

//! Shared fixtures for the P4.7 cron/scenario test harness (ADR-0005
//! Behavior Test Matrix, P4 rows).
//!
//! Includes the durable-sleep scenario primitives the FW rows need:
//! - [`boot_server`] / [`wait_ready`]: a real `resonate serve` (the cron
//!   manager is a durable-sleep loop — it needs the server's timer loop, not
//!   `Resonate::local`).
//! - [`cron_registry`]: a [`CronRegistry`] DI seam over a test cron set.
//! - [`housekeeper_workflow`]: the canonical P4 fixture workflow the cron
//!   manager fires on each tick.
//! - [`next_tick`]: small [`next_due`]-based clock helper for catch-up/
//!   restart timing.
//!
//! Usage: each scenario file does `#[path = "common/mod.rs"] mod common;`
//! and calls into it. These are FIXTURES, not assertions — the scenario
//! files own their behavior checks.

use std::process::Command;
use std::sync::Arc;
use std::time::Duration;
use toron_core::testkit::ChildGuard;

use toron_workflow::cron::{CronRegistration, CronRegistry};
use toron_workflow::engine::{ActionInvocation, AsyncEffectRunner, ProfileResolver};
use toron_workflow::schema::CronScope;
use toron_workflow::schema::{Action, parse_workflow};

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

/// A free localhost port for the `resonate serve` child.
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Boot a persistent `resonate serve` (sqlite) in `dir`. The child is the
/// server; dropping it must NOT lose durable state (the restart rows rely on
/// this — the server outlives the worker).
/// `true` when the external `resonate` binary is installed (GS-018:
/// hosted CI runners lack the herdr/resonate stack).
pub fn has_resonate() -> bool {
    Command::new("resonate")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Boot a persistent `resonate serve` (sqlite) in `dir`. The child is the
/// server; dropping it must NOT lose durable state (the restart rows rely on
/// this — the server outlives the worker).
pub fn boot_server(port: u16, dir: &std::path::Path) -> Option<ChildGuard> {
    // bd-6h9l: the guard kills and reaps at scope end, so a failing
    // assertion or a cancelled test binary cannot orphan the server.
    toron_core::testkit::ChildGuard::resonate_serve(port, dir).expect("spawn resonate serve")
}

/// Block until the server's HTTP port accepts TCP (bounded poll).
pub async fn wait_ready(url: &str) {
    for _ in 0..100 {
        let url = url.to_string();
        if tokio::task::spawn_blocking(move || {
            std::net::TcpStream::connect_timeout(
                &url.replace("http://", "").parse().expect("addr"),
                Duration::from_millis(200),
            )
            .is_ok()
        })
        .await
        .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("resonate server at {url} never became ready");
}

/// The canonical P4 fixture workflow: a single step that records a tick. The
/// FW rows drive this through [`CronManager`](toron_workflow::cron::CronManager).
/// The `send_dm` step is a stand-in "tick recorded" marker — the injected
/// no-op runner swallows it (assertions read the step output, not the DM).
///
/// bd-sc24-catchup-tick-never-fires-19xu: the recipient templates
/// `{{trigger.owner}}` — the field a
/// `schedule` trigger actually carries (`Trigger::Schedule { cron, owner,
/// scope }`). It must NOT be `{{trigger.sender}}` (a `message_posted`-only
/// field): the engine renders action fields STRICTLY (ADR-0032 Spec 4), so an
/// unresolvable ref fails the tick BEFORE the runner ever sees it — and the
/// runner is the only observable the cron rows have, so the loop would look
/// like it never fired.
pub fn housekeeper_workflow() -> toron_workflow::schema::Workflow {
    parse_workflow(
        r##"
name: housekeeper-tick
trigger:
  type: schedule
  cron: "* * * * *"
  owner: "@housekeeper"
steps:
  - name: record
    action: send_dm
    to: "{{trigger.owner}}"
    body: "tick"
"##,
    )
    .expect("parse housekeeper fixture workflow")
}

/// Build a [`CronRegistry`] DI seam over a fixed cron set (the embedding's
/// `crons.toml` half — flywheel bridges this in production; tests inject it).
pub fn cron_registry(crons: Vec<CronRegistration>) -> CronRegistry {
    Arc::new(move || crons.clone())
}

/// A [`CronRegistration`] for the fixture workflow. `cron` is the 5-field
/// expression; `enabled` gates firing. `next_due` validates the expr on the
/// fly so a typo fails fast at fixture-build time, not deep in a run.
pub fn fixture_registration(name: &str, cron: &str, enabled: bool) -> CronRegistration {
    assert!(
        toron_workflow::cron::next_due(cron, 1_700_000_000).is_some(),
        "fixture cron expression must parse: {cron}"
    );
    CronRegistration {
        name: name.to_string(),
        cron: cron.to_string(),
        workflow: housekeeper_workflow(),
        owner: "@housekeeper".to_string(),
        scope: CronScope::default(),
        enabled,
    }
}

/// Seconds until the next due tick at-or-after `from_ts` (UTC). Convenience
/// wrapper over [`next_due`] for catch-up/restart timing math.
pub fn next_tick(cron: &str, from_ts: i64) -> Option<i64> {
    toron_workflow::cron::next_due(cron, from_ts)
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
