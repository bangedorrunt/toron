// governed-by: ADR-0003
//! bd-6h9l fence: the e2e server fixture must not outlive its test.
//!
//! 54 orphaned `resonate serve` processes were measured on one host, all
//! `ppid=1`, holding 738 MB. `Child::kill` sends SIGKILL, which a child cannot
//! ignore, so every one of those survivors is proof that the releasing
//! statement was never reached — the harnesses killed the server in a trailing
//! line AFTER their assertions, and a failing assertion or a cancelled test
//! binary skips it entirely.
//!
//! This exercises the real fixture (`common::boot_server`) through the path
//! that used to leak: a scope that panics. The process census is the same one
//! that found the orphans, so the fence measures the actual symptom rather than
//! a proxy.

mod common;

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// Every `resonate serve` pid currently on this host.
fn resonate_pids() -> HashSet<u32> {
    let out = std::process::Command::new("pgrep")
        .args(["-f", "resonate serve"])
        .output();
    match out {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .split_whitespace()
            .filter_map(|p| p.parse().ok())
            .collect(),
        _ => HashSet::new(),
    }
}

fn scratch_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("toron-orphan-{}-{}", tag, std::process::id()));
    let _ = std::fs::create_dir_all(&d);
    d
}

/// A free loopback port for the fixture.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

#[test]
fn a_panicking_scenario_leaves_no_orphaned_server() {
    if !toron_core::testkit::resonate_installed() {
        eprintln!("SKIP: no `resonate` binary (needs the herdr/resonate stack)");
        return;
    }

    let before = resonate_pids();
    let port = free_port();
    let dir = scratch_dir("panic");
    let seen = Arc::new(AtomicU32::new(0));

    // The exact shape that leaked: a scenario that fails mid-body. The server
    // is held by the scope, and the scope unwinds through a panic.
    let recorded = Arc::clone(&seen);
    let dir_for_scope = dir.clone();
    let result = std::panic::catch_unwind(move || {
        let server =
            common::boot_server(port, &dir_for_scope).expect("boot_server returns None here");
        recorded.store(server.pid().unwrap_or(0), Ordering::SeqCst);
        // The trailing `server.kill()` the old harnesses relied on is gone —
        // cleanup is the guard's Drop, which a panic still runs.
        assert_eq!(1, 2, "deliberate mid-test assertion failure");
    });

    assert!(result.is_err(), "the scenario really did panic");
    let pid = seen.load(Ordering::SeqCst);
    assert!(pid != 0, "the fixture spawned a server before the panic");

    // The server needs a moment to die; SIGKILL is immediate but the census
    // is a fresh process spawn.
    let mut leaked = Vec::new();
    for _ in 0..50 {
        leaked = resonate_pids()
            .difference(&before)
            .copied()
            .filter(|p| *p == pid)
            .collect();
        if leaked.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(
        leaked.is_empty(),
        "a panicking scenario orphaned resonate serve pid {pid:?} (ppid=1, unreapable)"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_passing_scenario_leaves_no_orphaned_server() {
    if !toron_core::testkit::resonate_installed() {
        eprintln!("SKIP: no `resonate` binary (needs the herdr/resonate stack)");
        return;
    }

    let before = resonate_pids();
    let port = free_port();
    let dir = scratch_dir("pass");
    {
        let server = common::boot_server(port, &dir).expect("boot_server returns None here");
        assert!(server.pid().is_some(), "the fixture reports a pid");
        // scope ends here; the guard releases the server
    }

    let mut leaked = Vec::new();
    for _ in 0..50 {
        leaked = resonate_pids().difference(&before).copied().collect();
        if leaked.is_empty() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    assert!(
        leaked.is_empty(),
        "a passing scenario left orphans behind: {leaked:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
