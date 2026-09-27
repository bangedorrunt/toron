// governed-by: ADR-0031
//! Chart shaping, pure (`&[..] -> Vec<u64>`). No ratatui types: the view maps
//! these numbers onto `Sparkline` / `Bar` / block characters, so the shapes
//! stay testable without a terminal.

/// Seconds in an hour.
pub const HOUR: u64 = 3_600;

/// Count timestamps into `window_hours` hourly buckets, oldest first, ending
/// with the bucket that contains `now`. Events outside the window are dropped
/// (they cannot be shown), events in the future land in the newest bucket.
pub fn hour_buckets(timestamps: &[u64], now: u64, window_hours: u64) -> Vec<u64> {
    let hours = window_hours.max(1) as usize;
    let mut buckets = vec![0u64; hours];
    let span_start = now.saturating_sub((hours as u64 - 1) * HOUR) / HOUR * HOUR;
    for ts in timestamps {
        if *ts < span_start {
            continue;
        }
        let idx = (((ts - span_start) / HOUR) as usize).min(hours - 1);
        buckets[idx] += 1;
    }
    buckets
}

/// Scale a series to bar widths in `[0, max_width]`, keeping the largest bar
/// full. A zero series stays zero (no fake baseline).
pub fn bar_widths(values: &[u64], max_width: u16) -> Vec<u16> {
    let max = values.iter().copied().max().unwrap_or(0);
    if max == 0 || max_width == 0 {
        return vec![0; values.len()];
    }
    values
        .iter()
        .map(|v| ((*v as u128 * max_width as u128) / max as u128) as u16)
        .collect()
}

/// Compact count for stat cards: `999` / `1.2k` / `3.4M`.
pub fn compact(n: u64) -> String {
    if n < 1_000 {
        return n.to_string();
    }
    if n < 1_000_000 {
        return format!("{:.1}k", n as f64 / 1_000.0);
    }
    format!("{:.1}M", n as f64 / 1_000_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hour_buckets_window_and_edges() {
        let now = 100 * HOUR + 60; // one minute into hour 100
        // The 3-hour window is [98, 99, 100]: one event this hour, one two
        // hours ago, and one five hours ago (outside the window entirely).
        let ts = vec![now, now - 2 * HOUR, now - 5 * HOUR, now - HOUR];
        let buckets = hour_buckets(&ts, now, 3);
        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets, vec![1, 1, 1], "oldest bucket first, now last");
        // out-of-window events are dropped, never counted into a wrong bucket
        assert_eq!(
            hour_buckets(&[now - 5 * HOUR], now, 3),
            vec![0, 0, 0],
            "an event older than the window is not shown"
        );
        // a future timestamp clamps into the newest bucket instead of panicking
        assert_eq!(hour_buckets(&[now + 10 * HOUR], now, 3), vec![0, 0, 1]);
        // empty window never divides by zero
        assert_eq!(hour_buckets(&[], now, 1), vec![0]);
    }

    #[test]
    fn bar_widths_keep_the_peak_full() {
        assert_eq!(bar_widths(&[1, 2, 4], 10), vec![2, 5, 10]);
        assert_eq!(bar_widths(&[0, 0], 10), vec![0, 0]);
        assert_eq!(bar_widths(&[], 10), Vec::<u16>::new());
        // max_width 0 is a no-op, never a division by zero
        assert_eq!(bar_widths(&[3], 0), vec![0]);
    }

    #[test]
    fn compact_is_guarded() {
        assert_eq!(compact(0), "0");
        assert_eq!(compact(999), "999");
        assert_eq!(compact(1_500), "1.5k");
        assert_eq!(compact(2_500_000), "2.5M");
    }
}
