// governed-by: ADR-0031
// Non-Linux process inspection (bd-nc5d): no `/proc` — callers fall through
// to the portable `ps` probe. Only compiled off Linux — zero cfg here.

/// Always `false` off Linux: no fast path, use the `ps` fallback.
pub(crate) fn proc_cmdline_contains(_pid: u32, _needle: &str) -> bool {
    false
}
