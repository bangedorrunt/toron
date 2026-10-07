// governed-by: ADR-0034 (G5)
//! Shared timeout/retry vocabulary: one bounded-wait type, one retry
//! policy, drivers that sleep at the edge only. New remote edges take these
//! as values (injected, never ambient constants); delay math stays pure so
//! tests substitute without sleeping real seconds.

use std::time::{Duration, Instant};

/// A bounded wait: deadline plus poll step. Timeout inside, retry outside.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timeout {
    pub deadline: Duration,
    pub poll: Duration,
}

impl Timeout {
    pub const fn new(deadline: Duration) -> Self {
        Self {
            deadline,
            poll: Duration::from_millis(10),
        }
    }

    pub const fn with_poll(mut self, poll: Duration) -> Self {
        self.poll = poll;
        self
    }

    pub fn expired(&self, start: Instant) -> bool {
        start.elapsed() >= self.deadline
    }
}

/// Retry policy: cap + exponential backoff + optional jitter. Delays are
/// pure values (`delay_for`); only the drivers sleep.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base: Duration,
    pub factor: f64,
    pub max_delay: Duration,
    pub jitter: bool,
    /// Per-policy entropy for jitter, filled ONCE at construction (the
    /// embedding's edge). `delay_for` reads it as data, never the clock, so
    /// the delay math stays pure and reproducible; two processes with
    /// different seeds spread their retries instead of moving in lock-step.
    pub jitter_seed: u64,
}

impl RetryPolicy {
    /// Delay after `attempt` (1-based) just failed.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let growth = self.factor.powi(attempt.saturating_sub(1) as i32);
        let mut delay = self.base.mul_f64(growth.max(0.0));
        if delay > self.max_delay {
            delay = self.max_delay;
        }
        if self.jitter {
            delay = apply_jitter(delay, attempt, self.jitter_seed);
        }
        delay
    }

    /// True while another try is allowed after `attempt` failures.
    pub const fn should_retry(&self, failed_attempts: u32) -> bool {
        failed_attempts < self.max_attempts
    }
}

/// Sync retry driver: total tries never exceed `max_attempts`.
pub fn retry_sync<T, E>(
    policy: &RetryPolicy,
    mut op: impl FnMut() -> Result<T, E>,
) -> Result<T, E> {
    let mut failed = 0u32;
    loop {
        match op() {
            Ok(value) => return Ok(value),
            Err(e) => {
                failed += 1;
                if !policy.should_retry(failed) {
                    return Err(e);
                }
                std::thread::sleep(policy.delay_for(failed));
            }
        }
    }
}

/// Async retry driver (tokio sleep at the edge only).
pub async fn retry_async<T, E, F, Fut>(policy: &RetryPolicy, mut op: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut failed = 0u32;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(e) => {
                failed += 1;
                if !policy.should_retry(failed) {
                    return Err(e);
                }
                tokio::time::sleep(policy.delay_for(failed)).await;
            }
        }
    }
}

/// ±25% jitter via a cheap xorshift (no new dependency for a few lines).
/// Pure: the only entropy is the caller-supplied `seed` (the policy's
/// `jitter_seed`, filled once at construction), so `delay_for` never reads a
/// clock and the same seed reproduces the same delay. Two policies with
/// different seeds spread their retries instead of moving in lock-step.
fn apply_jitter(delay: Duration, attempt: u32, seed: u64) -> Duration {
    let mut x = (attempt as u64)
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .wrapping_add(seed | 1);
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    let unit = (x >> 11) as f64 / (u64::MAX >> 11) as f64;
    delay.mul_f64(0.5f64.mul_add(unit, 0.75))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn delay_caps_at_max_delay() {
        let p = RetryPolicy {
            max_attempts: 5,
            base: Duration::from_secs(1),
            factor: 10.0,
            max_delay: Duration::from_secs(5),
            jitter: false,
            jitter_seed: 0,
        };
        assert_eq!(p.delay_for(1), Duration::from_secs(1));
        assert_eq!(p.delay_for(3), Duration::from_secs(5), "capped");
    }

    #[test]
    fn fail_twice_then_succeeds() {
        let p = RetryPolicy {
            max_attempts: 5,
            base: Duration::ZERO,
            factor: 2.0,
            max_delay: Duration::from_secs(1),
            jitter: false,
            jitter_seed: 0,
        };
        let mut calls = 0u32;
        let r = retry_sync(&p, || {
            calls += 1;
            if calls < 3 {
                Err("flaky")
            } else {
                Ok("settled")
            }
        });
        assert_eq!(r, Ok("settled"));
        assert_eq!(calls, 3);
    }

    #[test]
    fn storm_stops_at_cap() {
        let p = RetryPolicy {
            max_attempts: 4,
            base: Duration::ZERO,
            factor: 2.0,
            max_delay: Duration::from_secs(1),
            jitter: false,
            jitter_seed: 0,
        };
        let mut calls = 0u32;
        let r: Result<(), &str> = retry_sync(&p, || {
            calls += 1;
            Err("storm")
        });
        assert!(r.is_err());
        assert_eq!(calls, 4, "exactly max_attempts tries");
    }

    #[test]
    fn timeout_expiry_is_monotonic() {
        let t = Timeout::new(Duration::from_millis(1));
        assert!(!t.expired(std::time::Instant::now()), "fresh start");
        std::thread::sleep(Duration::from_millis(5));
        let t0 = Timeout::new(Duration::from_millis(1));
        let start = std::time::Instant::now()
            .checked_sub(Duration::from_secs(1))
            .expect("past deadline is representable");
        assert!(t0.expired(start), "past deadline");
        let _ = t;
    }

    #[test]
    fn jitter_stays_in_band() {
        let p = RetryPolicy {
            max_attempts: 3,
            base: Duration::from_secs(1),
            factor: 1.0,
            max_delay: Duration::from_secs(10),
            jitter: true,
            jitter_seed: 0,
        };
        for attempt in 1..=50 {
            let d = p.delay_for(attempt);
            assert!(
                d >= Duration::from_millis(750) && d <= Duration::from_millis(1250),
                "attempt {attempt}: {d:?} in [0.75x, 1.25x]"
            );
        }
    }

    /// bd-35el: the jitter is a pure function of (delay, attempt, seed) — no
    /// clock read. Same seed reproduces the same schedule; different seeds
    /// diverge, which is the only reason to jitter at all.
    #[test]
    fn jitter_is_deterministic_per_seed() {
        let make = |seed: u64| RetryPolicy {
            max_attempts: 3,
            base: Duration::from_secs(1),
            factor: 1.0,
            max_delay: Duration::from_secs(10),
            jitter: true,
            jitter_seed: seed,
        };
        let a = make(0xA11CE);
        let b = make(0xA11CE);
        for attempt in 1..=50 {
            assert_eq!(
                a.delay_for(attempt),
                b.delay_for(attempt),
                "same seed -> same delay (attempt {attempt})"
            );
        }
        let c = make(0xB0B);
        assert!(
            (1..=50).any(|i| a.delay_for(i) != c.delay_for(i)),
            "different seeds produce different schedules"
        );
    }
}
