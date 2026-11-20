// governed-by: ADR-0039 U6 (KTD8, KTD4)
#![forbid(unsafe_code)]

//! Pure webhook retry policy (U6). No loop runs outside the kernel: `decide`
//! is total, takes the clock and jitter as data, and returns a typed decision.
//! Timers enter as clock Msgs; a failed delivery returns `Retry { delay_ms }`
//! and the kernel schedules it. Mirrors elm-effect-rs `Schedule` algebra.

/// The one HTTP outcome the policy reasons about (no response body/secret).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    Status(u16, Option<u64>), // status, Retry-After seconds when present
    Timeout,
    Transport,
}

/// What the kernel must do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    Delivered,
    /// Terminal: 410 → subscription terminated; 413 → event dropped (recorded).
    Terminated(&'static str),
    /// Exhausted attempts: queue a gap, keep the subscription Active.
    Gap,
    Retry {
        delay_ms: u64,
    },
}

#[derive(Clone, Debug)]
pub struct RetryPolicy {
    pub base_ms: u64,
    pub cap_ms: u64,
    pub max_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            base_ms: 5_000,
            cap_ms: 30 * 60 * 1_000,
            max_attempts: 8,
        }
    }
}

/// Exponential backoff with bounded jitter: `base * 2^(attempt-1)`, capped,
/// then a jitter in `[0, backoff)` added and clamped to the cap. `rand_unit`
/// is supplied by the caller in `[0,1)` so tests are deterministic.
pub fn delay_ms(attempt: u32, rand_unit: f64, policy: &RetryPolicy) -> u64 {
    let shift = attempt.saturating_sub(1).min(20);
    let backoff = policy
        .base_ms
        .saturating_mul(1u64 << shift)
        .min(policy.cap_ms);
    let jitter = ((rand_unit.clamp(0.0, 0.9999)) * backoff as f64) as u64;
    backoff.saturating_add(jitter).min(policy.cap_ms)
}

/// The `given` predicate: 410 and 413 are never retried.
pub fn retryable(outcome: &Outcome) -> bool {
    !matches!(outcome, Outcome::Status(410, _) | Outcome::Status(413, _))
}

/// Decide the next step after `attempt` (1-based) produced `outcome`.
pub fn decide(attempt: u32, outcome: &Outcome, rand_unit: f64, policy: &RetryPolicy) -> Decision {
    match outcome {
        Outcome::Ok => Decision::Delivered,
        Outcome::Status(410, _) => Decision::Terminated("gone"),
        Outcome::Status(413, _) => Decision::Terminated("too-large"),
        Outcome::Status(code, retry_after) if (429..=499).contains(code) || *code >= 500 => {
            if attempt >= policy.max_attempts {
                return Decision::Gap;
            }
            let base = delay_ms(attempt, rand_unit, policy);
            let delay = match retry_after {
                Some(secs) => (*secs as u64)
                    .saturating_mul(1000)
                    .clamp(base.min(policy.cap_ms), policy.cap_ms),
                None => base,
            };
            Decision::Retry { delay_ms: delay }
        }
        Outcome::Status(code, _) if (200..=399).contains(code) => Decision::Delivered,
        Outcome::Timeout | Outcome::Transport | Outcome::Status(_, _) => {
            if attempt >= policy.max_attempts {
                return Decision::Gap;
            }
            Decision::Retry {
                delay_ms: delay_ms(attempt, rand_unit, policy),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ae4_a_503_then_200_delivers_on_the_second_attempt() {
        let p = RetryPolicy::default();
        let first = decide(1, &Outcome::Status(503, None), 0.0, &p);
        assert!(
            matches!(first, Decision::Retry { delay_ms: 5_000 }),
            "{first:?}"
        );
        let second = decide(2, &Outcome::Ok, 0.0, &p);
        assert_eq!(second, Decision::Delivered);
    }

    #[test]
    fn a_410_terminates_with_one_post_and_413_drops_without_retry() {
        let p = RetryPolicy::default();
        assert_eq!(
            decide(1, &Outcome::Status(410, None), 0.0, &p),
            Decision::Terminated("gone")
        );
        assert_eq!(
            decide(1, &Outcome::Status(413, None), 0.0, &p),
            Decision::Terminated("too-large")
        );
        assert!(!retryable(&Outcome::Status(410, None)));
        assert!(!retryable(&Outcome::Status(413, None)));
        assert!(retryable(&Outcome::Status(503, None)));
    }

    #[test]
    fn retry_after_is_honoured_within_the_cap() {
        let p = RetryPolicy::default();
        let d = decide(1, &Outcome::Status(429, Some(9)), 0.0, &p);
        assert_eq!(d, Decision::Retry { delay_ms: 9_000 });
        let d = decide(1, &Outcome::Status(429, Some(999_999)), 0.0, &p);
        assert_eq!(d, Decision::Retry { delay_ms: p.cap_ms });
    }

    #[test]
    fn after_eight_failures_a_gap_is_queued_not_a_ninth_post() {
        let p = RetryPolicy::default();
        assert_eq!(decide(8, &Outcome::Timeout, 0.0, &p), Decision::Gap);
        assert!(matches!(
            decide(7, &Outcome::Timeout, 0.0, &p),
            Decision::Retry { .. }
        ));
    }

    #[test]
    fn a_fixed_seed_matches_the_capped_exponential_sequence() {
        let p = RetryPolicy {
            base_ms: 5_000,
            cap_ms: 30 * 60 * 1_000,
            max_attempts: 8,
        };
        // attempt 1: 5s; 2: 10s; 3: 20s; 4: 40s; with rand_unit 0 no jitter.
        assert_eq!(delay_ms(1, 0.0, &p), 5_000);
        assert_eq!(delay_ms(2, 0.0, &p), 10_000);
        assert_eq!(delay_ms(3, 0.0, &p), 20_000);
        assert_eq!(delay_ms(4, 0.0, &p), 40_000);
        assert_eq!(delay_ms(30, 0.0, &p), p.cap_ms);
    }
}
