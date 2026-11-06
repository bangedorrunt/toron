// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! One temporal type for CLI `--since` edges (wave5-b).
//!
//! Parsing is lenient-but-unambiguous: RFC3339 timestamps, relative offsets
//! (`30s` / `15m` / `6h` / `7d`), and the literal `last` (a caller-held
//! high-water cursor). Anything else is a typed [`TimeSpecError`] that names
//! the accepted forms — never a silent zero, never a guessed epoch.

use std::fmt;
use std::time::Duration;

/// A parsed `--since` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeSpec {
    /// Absolute RFC3339 timestamp, as unix seconds (never negative).
    Rfc3339(u64),
    /// Offset back from now.
    Relative(Duration),
    /// The caller's high-water cursor, resolved by the caller.
    Last,
}

/// Why a `--since` value was rejected, or why `last` cannot resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimeSpecError {
    /// Not RFC3339, not a relative offset, not `last`.
    Unparseable { input: String },
    /// `last` with no high-water cursor available.
    NoHighWater,
}

impl fmt::Display for TimeSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unparseable { input } => write!(
                f,
                "bad timestamp {input:?}: expected an RFC3339 timestamp (e.g. \
                 2026-08-18T00:00:00Z), a relative offset (30s, 15m, 6h, 7d), \
                 or `last`"
            ),
            Self::NoHighWater => write!(
                f,
                "`last` needs a high-water cursor, but this command tracks none — \
                 pass an RFC3339 timestamp or a relative offset instead"
            ),
        }
    }
}

impl std::error::Error for TimeSpecError {}

/// Accepted relative suffixes: seconds, minutes, hours, days.
fn relative_duration(s: &str) -> Option<Duration> {
    let (digits, suffix) = s.split_at_checked(s.len().checked_sub(1)?)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    match suffix {
        "s" => Some(Duration::from_secs(n)),
        "m" => Some(Duration::from_secs(n.saturating_mul(60))),
        "h" => Some(Duration::from_secs(n.saturating_mul(3_600))),
        "d" => Some(Duration::from_secs(n.saturating_mul(86_400))),
        _ => None,
    }
}

/// Parse one `--since` value. Surrounding whitespace is tolerated; `last`
/// is exact lowercase (the daemon cursor matches it verbatim).
pub fn parse(s: &str) -> Result<TimeSpec, TimeSpecError> {
    let t = s.trim();
    if t == "last" {
        return Ok(TimeSpec::Last);
    }
    if let Some(d) = relative_duration(t) {
        return Ok(TimeSpec::Relative(d));
    }
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(t) {
        return Ok(TimeSpec::Rfc3339(dt.timestamp().max(0) as u64));
    }
    Err(TimeSpecError::Unparseable {
        input: s.to_string(),
    })
}

impl TimeSpec {
    /// Resolve to unix seconds. `Relative` counts back from `now_secs`
    /// (saturating, never negative); `Last` reads the caller's high-water
    /// and fails loudly when there is none — never 0.
    pub fn resolve_since(
        self,
        now_secs: u64,
        last_water: Option<u64>,
    ) -> Result<u64, TimeSpecError> {
        match self {
            Self::Rfc3339(t) => Ok(t),
            Self::Relative(d) => Ok(now_secs.saturating_sub(d.as_secs())),
            Self::Last => last_water.ok_or(TimeSpecError::NoHighWater),
        }
    }
}

/// Resolve a raw `--since` argument to the daemon-facing string: `None`
/// stays `None`, `last` passes through verbatim (the daemon owns the
/// cursor), RFC3339 passes through byte-identical, and relative offsets
/// resolve against `now_secs` to an RFC3339 timestamp the daemon already
/// parses. Garbage fails here, loudly, before any store is touched.
pub fn resolve_since_arg(s: Option<&str>, now_secs: u64) -> Result<Option<String>, TimeSpecError> {
    match s {
        None => Ok(None),
        Some(raw) => match parse(raw)? {
            TimeSpec::Last => Ok(Some("last".to_string())),
            TimeSpec::Rfc3339(_) => Ok(Some(raw.trim().to_string())),
            TimeSpec::Relative(d) => {
                let t = now_secs.saturating_sub(d.as_secs());
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(t as i64, 0)
                    .unwrap_or_else(chrono::Utc::now);
                Ok(Some(dt.to_rfc3339()))
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_rfc3339_relative_and_last() {
        assert!(matches!(
            parse("2026-08-18T00:00:00Z"),
            Ok(TimeSpec::Rfc3339(_))
        ));
        assert!(matches!(
            parse("2026-08-18T00:00:00+02:00"),
            Ok(TimeSpec::Rfc3339(_))
        ));
        assert_eq!(
            parse("30s"),
            Ok(TimeSpec::Relative(Duration::from_secs(30)))
        );
        assert_eq!(
            parse("15m"),
            Ok(TimeSpec::Relative(Duration::from_secs(900)))
        );
        assert_eq!(
            parse("6h"),
            Ok(TimeSpec::Relative(Duration::from_secs(21_600)))
        );
        assert_eq!(
            parse("7d"),
            Ok(TimeSpec::Relative(Duration::from_secs(604_800)))
        );
        assert_eq!(parse("0s"), Ok(TimeSpec::Relative(Duration::from_secs(0))));
        assert_eq!(
            parse("  6h  "),
            Ok(TimeSpec::Relative(Duration::from_secs(21_600)))
        );
        assert_eq!(parse("last "), Ok(TimeSpec::Last)); // surrounding space trims
        assert_eq!(parse("last"), Ok(TimeSpec::Last));
    }

    #[test]
    fn rejects_everything_else_with_the_accepted_forms() {
        for bad in [
            "",
            "   ",
            "6hours",
            "1w",
            "6H",
            "6h30m",
            "-5m",
            "tomorrow",
            "123",
            "LAST",
            "99999999999999999999999s",
        ] {
            let err = parse(bad).expect_err(&format!("{bad:?} must fail"));
            let msg = err.to_string();
            assert!(msg.contains("RFC3339"), "names RFC3339: {msg}");
            assert!(msg.contains("30s"), "names relative forms: {msg}");
            assert!(msg.contains("`last`"), "names last: {msg}");
        }
    }

    #[test]
    fn resolve_never_silently_zeroes() {
        assert_eq!(
            TimeSpec::Rfc3339(1_700_000_000).resolve_since(9_999, None),
            Ok(1_700_000_000)
        );
        assert_eq!(
            TimeSpec::Relative(Duration::from_secs(60)).resolve_since(1_000, None),
            Ok(940)
        );
        // underflow saturates, never wraps
        assert_eq!(
            TimeSpec::Relative(Duration::from_secs(9_999_999)).resolve_since(60, None),
            Ok(0)
        );
        assert_eq!(TimeSpec::Last.resolve_since(1_000, Some(42)), Ok(42));
        assert_eq!(
            TimeSpec::Last.resolve_since(1_000, None),
            Err(TimeSpecError::NoHighWater)
        );
    }

    #[test]
    fn resolve_since_arg_round_trips_shapes() {
        assert_eq!(resolve_since_arg(None, 1_000), Ok(None));
        assert_eq!(
            resolve_since_arg(Some("last"), 1_000),
            Ok(Some("last".to_string()))
        );
        assert_eq!(
            resolve_since_arg(Some("2026-08-18T00:00:00Z"), 1_000),
            Ok(Some("2026-08-18T00:00:00Z".to_string()))
        );
        let rel = resolve_since_arg(Some("6h"), 1_700_000_000).expect("relative resolves");
        assert!(rel.is_some() && rel.unwrap().ends_with("+00:00"));
        assert!(resolve_since_arg(Some("6hours"), 1_000).is_err());
    }
}
