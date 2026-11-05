// governed-by: ADR-0031

use std::fs;

/// Whether `/proc/<pid>/cmdline` mentions `needle` (fast path before the
/// portable `ps` fallback in `serve_lock.rs`).
pub(crate) fn proc_cmdline_contains(pid: u32, needle: &str) -> bool {
    if let Ok(cmdline) = fs::read(format!("/proc/{pid}/cmdline")) {
        let text = String::from_utf8_lossy(&cmdline);
        if text.contains(needle) {
            return true;
        }
    }
    false
}
