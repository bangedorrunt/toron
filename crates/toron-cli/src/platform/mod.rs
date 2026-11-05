// governed-by: ADR-0031

use std::path::Path;

/// How the daemon is currently supervised / running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DaemonState {
    /// Running under launchd (macOS).
    #[allow(dead_code)]
    RunningLaunchd,
    /// Running under systemd (Linux).
    #[allow(dead_code)]
    RunningSystemd,
    /// Running via pidfile spawn (fallback).
    RunningPidfile(u32),
    /// Address responds but no managed pidfile/unit — likely an unmanaged
    /// `toron serve` (e.g. the flywheel-managed daemon).
    RunningUnmanaged,
    Stopped,
}

/// Shared install inputs: `daemon.rs` builds this once from its prelude,
/// the active OS module consumes what it needs.
pub(crate) struct InstallCtx<'a> {
    pub label: &'a str,
    pub binary: &'a Path,
    pub addr: &'a str,
    pub key_path: &'a Path,
    pub pass: &'a Path,
    pub out_log: &'a Path,
    pub err_log: &'a Path,
    #[allow(dead_code)]
    pub state: &'a Path,
}

#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "macos")]
pub(crate) use macos::*;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(crate) use linux::*;

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
mod fallback;
#[cfg(not(any(target_os = "macos", target_os = "linux")))]
pub(crate) use fallback::*;
