// governed-by: ADR-0031

/// Always `false` off Linux: no fast path, use the `ps` fallback.
pub(crate) const fn proc_cmdline_contains(_pid: u32, _needle: &str) -> bool {
    false
}
