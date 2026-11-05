// governed-by: ADR-0035
//! Tower armor for the mail-send interpreter (bd-qdup, ADR-0035 Spec 2.7).
//!
//! OUTSIDE-IN ONLY, in this order — Timeout inner, Bulkhead,
//! Breaker, Fallback outer. No hand-rolled retry/timeout loops, no
//! resilience inside business logic:
//!
//! - [`tower::timeout::Timeout`] bounds one attempt (a hang becomes an
//!   elapsed error, which maps to `Ambiguous` — never a proven failure and
//!   never a silent stall).
//! - [`tower::limit::ConcurrencyLimit`] is the bulkhead (bounded parallel
//!   sends; overflow waits on the semaphore, never spawns).
//! - [`CircuitBreaker`] fails fast on a storm (consecutive-failure threshold,
//!   reset timeout, half-open probe) with the named [`ArmorError::BreakerOpen`].
//! - Fallback is explicit at the call site: `BreakerOpen` maps to a degraded
//!   value the journal settles as `unknown` — visible, never silent, never
//!   fake-success.
//!
//! RETRY IS DELIBERATELY ABSENT: a wire send is not idempotent (a blind
//! re-fire duplicates mail). Retry lives one level up, in the journaled
//! attempt planner, where a same-key retry ATTACHES to the stored outcome
//! instead of re-firing (proven by `prod_effect_fencing_*`). An armor that
//! retried here would defeat that guarantee — the six-scenario matrix pins
//! single-call on every failure path.
//!
//! Concrete types throughout (bd-qdup note): the stack composes tower
//! services over one boxed inner service, so there is exactly one
//! spelling of the path and no generic-resolution fragility.

use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use tower::{Service, ServiceExt};

/// What the armor protects: one send, performed by the boxed closure.
/// Boxed so deterministic fakes drive the six-scenario matrix without a
/// live mailbox; production wires the real `mailbox.send_message`.
#[derive(Clone)]
pub struct SendRequest {
    /// Performs exactly one send. Must be honest: `Ok` count on delivery,
    /// `Err` text on failure (the host classifies proven vs ambiguous).
    pub send: Arc<dyn Fn() -> SendFuture + Send + Sync>,
}

/// Boxed send future (bounded by the armor, never by the caller).
pub type SendFuture = Pin<Box<dyn std::future::Future<Output = Result<usize, String>> + Send>>;

/// Armor failure: every variant maps onto a journal outcome without
/// inventing success.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ArmorError {
    /// Transport-level failure inside the stack (timeout elapsed, semaphore
    /// closed): the send may or may not have happened.
    Ambiguous(String),
    /// The breaker is open: refused WITHOUT touching the inner service.
    BreakerOpen,
}

impl std::fmt::Display for ArmorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ambiguous(e) => write!(f, "ambiguous send failure: {e}"),
            Self::BreakerOpen => write!(f, "send breaker open"),
        }
    }
}

impl std::error::Error for ArmorError {}

/// Consecutive-failure threshold that opens the breaker (storm rule).
pub const BREAKER_THRESHOLD: u32 = 5;
/// How long an open breaker refuses before a half-open probe.
pub const BREAKER_RESET_AFTER: Duration = Duration::from_secs(30);

/// Breaker state: closed (flowing), open (failing fast until a deadline),
/// half-open (exactly one probe in flight — the `probe_in_flight` permit,
/// taken by `call`, refused to every other caller until it settles).
#[derive(Debug)]
enum BreakerState {
    Closed { consecutive_failures: u32 },
    Open { retry_at: tokio::time::Instant },
    HalfOpen { probe_in_flight: bool },
}

/// Breaker clock: production is wall time, tests inject a manual clock
/// (bd-mbck — no `tokio::time::Instant::now()` in the decision logic,
/// so recovery is deterministic with no real sleeps).
pub type ClockFn = Arc<dyn Fn() -> tokio::time::Instant + Send + Sync>;

/// The boxed inner send service: any send closure adapter.
type BoxSend = tower::util::BoxCloneService<SendRequest, Result<usize, String>, ArmorError>;

/// Adapt a send closure into the boxed service shape.
pub fn box_send(f: impl Fn(SendRequest) -> SendFuture + Send + Sync + Clone + 'static) -> BoxSend {
    tower::util::BoxCloneService::new(tower::service_fn(move |req: SendRequest| {
        let f = f.clone();
        async move { Ok::<_, ArmorError>((f)(req).await) }
    }))
}

/// Per-attempt send timeout inside the armor (bd-qdup): a hung send trips
/// here, never in a hand-rolled loop.
pub const ARMOR_SEND_TIMEOUT: Duration = Duration::from_secs(30);
/// Bulkhead width: at most this many sends proceed concurrently.
pub const ARMOR_MAX_CONCURRENCY: usize = 16;

/// The production inner service: runs the request's own send closure.
/// One shared instance serves a whole host (the breaker and bulkhead state
/// live in the armor around it, not in here).
pub fn passthrough() -> BoxSend {
    box_send(|req: SendRequest| (req.send)())
}

/// A circuit-breaker [`Service`] over the guarded stack (bd-qdup).
///
/// Counts consecutive failures (value-level `Err` and transport-level
/// errors alike — a storm is a storm); at the threshold it opens and calls
/// fail fast with [`ArmorError::BreakerOpen`] until the reset interval
/// passes, when exactly one probe goes through (success closes, failure
/// re-opens). Success resets the consecutive count. The lock guards only
/// the transition, never across an `.await`.
#[derive(Clone)]
pub struct CircuitBreaker {
    inner: tower::limit::ConcurrencyLimit<tower::timeout::Timeout<BoxSend>>,
    state: Arc<Mutex<BreakerState>>,
    threshold: u32,
    reset_after: Duration,
    clock: ClockFn,
}

impl std::fmt::Debug for CircuitBreaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CircuitBreaker")
            .field(
                "state",
                &self.state.lock().unwrap_or_else(PoisonError::into_inner),
            )
            .field("threshold", &self.threshold)
            .field("reset_after", &self.reset_after)
            .finish_non_exhaustive()
    }
}

/// Releases a HalfOpen probe permit when the probe future is dropped
/// before settling: caller cancellation must not wedge the breaker shut.
/// Completed probes mark `done` before `observe`, so the guard no-ops.
struct ProbeGuard {
    state: Arc<Mutex<BreakerState>>,
    probe: bool,
    done: Arc<AtomicBool>,
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        if !self.probe || self.done.load(Ordering::SeqCst) {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if matches!(
            *state,
            BreakerState::HalfOpen {
                probe_in_flight: true
            }
        ) {
            *state = BreakerState::HalfOpen {
                probe_in_flight: false,
            };
        }
    }
}

impl CircuitBreaker {
    /// New breaker over an inner send service (starts closed, production
    /// tuning from the named constants).
    pub fn new(inner: BoxSend, timeout: Duration, max_concurrency: usize) -> Self {
        Self::with_tuning(
            inner,
            timeout,
            max_concurrency,
            BREAKER_THRESHOLD,
            BREAKER_RESET_AFTER,
        )
    }

    /// Test/edge tuning: custom storm threshold and reset interval.
    /// Production always uses [`new`](Self::new) (the named constants).
    pub fn with_tuning(
        inner: BoxSend,
        timeout: Duration,
        max_concurrency: usize,
        threshold: u32,
        reset_after: Duration,
    ) -> Self {
        use tower::limit::ConcurrencyLimit;
        use tower::timeout::Timeout;
        Self {
            inner: ConcurrencyLimit::new(Timeout::new(inner, timeout), max_concurrency),
            state: Arc::new(Mutex::new(BreakerState::Closed {
                consecutive_failures: 0,
            })),
            threshold,
            reset_after,
            clock: Arc::new(tokio::time::Instant::now),
        }
    }

    /// Swap the breaker clock (tests inject a manual clock; production
    /// keeps the default wall time from `with_tuning`).
    pub fn with_clock(mut self, clock: ClockFn) -> Self {
        self.clock = clock;
        self
    }

    /// Observe one completed inner call. The clock is injected: `Open`
    /// deadlines and `HalfOpen` re-opens read `(self.clock)()`, never
    /// wall time directly (bd-mbck).
    fn observe(&self, failed: bool) {
        let threshold = self.threshold;
        let reset_after = self.reset_after;
        let now = (self.clock)();
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match (&mut *state, failed) {
            (
                BreakerState::Closed {
                    consecutive_failures,
                },
                true,
            ) => {
                *consecutive_failures += 1;
                if *consecutive_failures >= threshold {
                    *state = BreakerState::Open {
                        retry_at: now + reset_after,
                    };
                }
            }
            (
                BreakerState::Closed {
                    consecutive_failures,
                },
                false,
            ) => {
                *consecutive_failures = 0;
            }
            (BreakerState::HalfOpen { .. }, false) => {
                *state = BreakerState::Closed {
                    consecutive_failures: 0,
                };
            }
            (BreakerState::HalfOpen { .. }, true) => {
                *state = BreakerState::Open {
                    retry_at: now + reset_after,
                };
            }
            (BreakerState::Open { .. }, _) => {}
        }
    }
}

impl Service<SendRequest> for CircuitBreaker {
    type Response = Result<usize, String>;
    type Error = ArmorError;
    type Future = Pin<
        Box<dyn std::future::Future<Output = Result<Result<usize, String>, ArmorError>> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if let BreakerState::Open { retry_at } = *state {
                if (self.clock)() >= retry_at {
                    *state = BreakerState::HalfOpen {
                        probe_in_flight: false,
                    };
                } else {
                    return Poll::Ready(Err(ArmorError::BreakerOpen));
                }
            }
            if matches!(
                *state,
                BreakerState::HalfOpen {
                    probe_in_flight: true
                }
            ) {
                return Poll::Ready(Err(ArmorError::BreakerOpen));
            }
        }
        self.inner
            .poll_ready(cx)
            .map_err(|e| ArmorError::Ambiguous(e.to_string()))
    }

    fn call(&mut self, req: SendRequest) -> Self::Future {
        let probe = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            match *state {
                BreakerState::Open { .. } => {
                    return Box::pin(async { Err(ArmorError::BreakerOpen) });
                }
                BreakerState::HalfOpen {
                    probe_in_flight: true,
                } => {
                    return Box::pin(async { Err(ArmorError::BreakerOpen) });
                }
                BreakerState::HalfOpen {
                    probe_in_flight: false,
                } => {
                    *state = BreakerState::HalfOpen {
                        probe_in_flight: true,
                    };
                    true
                }
                BreakerState::Closed { .. } => false,
            }
        };
        let mut inner = self.inner.clone();
        let observer_state = self.state.clone();
        let observer = Self {
            inner: self.inner.clone(),
            state: observer_state,
            threshold: self.threshold,
            reset_after: self.reset_after,
            clock: self.clock.clone(),
        };
        Box::pin(async move {
            let done = Arc::new(AtomicBool::new(false));
            let _permit = ProbeGuard {
                state: observer.state.clone(),
                probe,
                done: done.clone(),
            };
            if let Err(e) = inner.ready().await {
                done.store(true, Ordering::SeqCst);
                observer.observe(true);
                return Err(ArmorError::Ambiguous(e.to_string()));
            }
            match inner.call(req).await {
                Ok(outcome) => {
                    done.store(true, Ordering::SeqCst);
                    observer.observe(outcome.is_err());
                    Ok(outcome)
                }
                Err(e) => {
                    done.store(true, Ordering::SeqCst);
                    observer.observe(true);
                    Err(ArmorError::Ambiguous(e.to_string()))
                }
            }
        })
    }
}

/// The full outside-in stack (bd-qdup): one shared breaker over the
/// timeout+bulkhead-guarded inner service, explicit fallback at the call
/// site. Built once per host and shared (`Clone + Send + Sync` via one
/// lock acquisition per send that snapshots the stack — the lock never
/// crosses an `.await`).
#[derive(Debug, Clone)]
pub struct SendArmor {
    stack: Arc<Mutex<CircuitBreaker>>,
}

impl SendArmor {
    /// New armor over an inner send service (production tuning).
    pub fn new(inner: BoxSend, timeout: Duration, max_concurrency: usize) -> Self {
        Self::with_tuning(
            inner,
            timeout,
            max_concurrency,
            BREAKER_THRESHOLD,
            BREAKER_RESET_AFTER,
        )
    }

    /// Test tuning: custom storm threshold and reset interval.
    pub fn with_tuning(
        inner: BoxSend,
        timeout: Duration,
        max_concurrency: usize,
        threshold: u32,
        reset_after: Duration,
    ) -> Self {
        Self {
            stack: Arc::new(Mutex::new(CircuitBreaker::with_tuning(
                inner,
                timeout,
                max_concurrency,
                threshold,
                reset_after,
            ))),
        }
    }

    /// Swap the breaker clock (tests inject a manual clock; production
    /// keeps the default wall time from `with_tuning`).
    pub fn with_clock(self, clock: ClockFn) -> Self {
        self.stack
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clock = clock;
        self
    }

    /// Run one send through the whole stack: timeout bounds the attempt, the
    /// bulkhead bounds parallelism, the breaker fails fast on storms, and a
    /// refused send maps to the explicit fallback value (never silent).
    /// Single-call guaranteed: nothing in the stack re-fires (retry lives
    /// in the journaled attempt planner).
    pub async fn send(&self, req: SendRequest) -> Result<Result<usize, String>, ArmorError> {
        let mut breaker = self
            .stack
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        breaker.ready().await?;
        breaker.call(req).await
    }
}

#[cfg(test)]
mod armor_matrix_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    /// One fake send service: counts calls, answers per the script.
    /// Scripts answer with VALUES (`Ok`), so classification stays with the
    /// caller — the armor never invents it.
    fn fake(
        calls: Arc<AtomicUsize>,
        f: impl Fn() -> Result<usize, String> + Send + Sync + Clone + 'static,
    ) -> BoxSend {
        let f = Arc::new(f);
        box_send(move |_req| {
            let f = f.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                f()
            }) as SendFuture
        })
    }

    fn req() -> SendRequest {
        SendRequest {
            send: Arc::new(|| Box::pin(async move { Ok(0) })),
        }
    }

    /// 1. Success passes through with exactly one inner call.
    #[tokio::test]
    async fn success_passes_through_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let armor = SendArmor::new(fake(calls.clone(), || Ok(7)), Duration::from_secs(5), 4);
        let out = armor.send(req()).await.expect("armor ok");
        drop(armor);
        assert_eq!(out, Ok(7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// 2. Proven failure escalates with exactly one inner call — the armor
    /// never retries a wire send (duplicates mail); retry lives in the
    /// journaled attempt planner.
    #[tokio::test]
    async fn proven_failure_escalates_without_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let armor = SendArmor::new(
            fake(calls.clone(), || Err("refused: no signer".into())),
            Duration::from_secs(5),
            4,
        );
        let out = armor.send(req()).await.expect("armor ok");
        drop(armor);
        assert_eq!(out, Err("refused: no signer".to_string()));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no blind retry on proven failure"
        );
    }

    /// 3. Ambiguous failure escalates as unknown with exactly one call —
    /// the value rides through unclassified (classification is the host's).
    #[tokio::test]
    async fn ambiguous_failure_escalates_unknown_without_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let armor = SendArmor::new(
            fake(calls.clone(), || Err("bus timeout".into())),
            Duration::from_secs(5),
            4,
        );
        let out = armor.send(req()).await.expect("armor ok");
        drop(armor);
        assert_eq!(out, Err("bus timeout".to_string()));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "no blind retry");
    }

    /// 4. A hung send trips the inner timeout and maps to Ambiguous — the
    /// caller waits milliseconds, never forever.
    #[tokio::test]
    async fn hang_times_out_to_ambiguous() {
        let inner = box_send(|_| Box::pin(async move { std::future::pending().await }));
        let armor = SendArmor::new(inner, Duration::from_millis(20), 4);
        let t0 = tokio::time::Instant::now();
        let err = armor.send(req()).await.expect_err("timeout must fail");
        drop(armor);
        assert!(
            matches!(err, ArmorError::Ambiguous(_)),
            "hang maps to Ambiguous, got {err:?}"
        );
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "bounded wait, took {:?}",
            t0.elapsed()
        );
    }

    /// 5. Storm: consecutive failures open the breaker; the next call fails
    /// fast WITHOUT touching the inner service.
    #[tokio::test]
    async fn storm_opens_breaker_and_fails_fast() {
        let calls = Arc::new(AtomicUsize::new(0));
        let armor = SendArmor::with_tuning(
            fake(calls.clone(), || Err("down".into())),
            Duration::from_secs(5),
            4,
            3,
            Duration::from_secs(60),
        );
        for _ in 0..3 {
            let _ = armor.send(req()).await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let err = armor.send(req()).await.expect_err("open breaker fails");
        drop(armor);
        assert_eq!(err, ArmorError::BreakerOpen);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "open breaker never touches inner"
        );
    }

    /// 6. Recovery: after the reset interval one probe goes through; its
    /// success closes the breaker and traffic flows again.
    #[tokio::test]
    async fn recovery_probe_closes_breaker() {
        let calls = Arc::new(AtomicUsize::new(0));
        let n = calls.clone();
        let inner = box_send(move |_| {
            let n = n.clone();
            Box::pin(async move {
                let c = n.fetch_add(1, Ordering::SeqCst);
                if c < 2 {
                    Err("down".to_string())
                } else {
                    Ok(1usize)
                }
            }) as SendFuture
        });
        let armor = SendArmor::with_tuning(
            inner,
            Duration::from_secs(5),
            4,
            2,
            Duration::from_millis(50),
        );
        let _ = armor.send(req()).await;
        let _ = armor.send(req()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        tokio::time::sleep(Duration::from_millis(70)).await;
        let out = armor.send(req()).await.expect("probe goes through");
        assert_eq!(out, Ok(1), "probe success closes the breaker");
        let out = armor.send(req()).await.expect("traffic flows");
        drop(armor);
        assert_eq!(out, Ok(1));
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    /// Controllable clock handle: `clock()` is the injected `ClockFn`,
    /// `advance` moves it. No tokio sleeps, no wall time.
    ///
    /// Manual test clock (bd-mbck): drives Open -> HalfOpen -> probe with
    /// no real sleeps. Production always uses the default system clock.
    #[derive(Clone)]
    struct ManualClock {
        now: Arc<Mutex<tokio::time::Instant>>,
    }

    impl ManualClock {
        fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(tokio::time::Instant::now())),
            }
        }

        fn clock(&self) -> ClockFn {
            let now = self.now.clone();
            Arc::new(move || *now.lock().unwrap_or_else(PoisonError::into_inner))
        }

        fn advance(&self, d: Duration) {
            let mut now = self.now.lock().unwrap_or_else(PoisonError::into_inner);
            *now += d;
        }
    }

    /// bd-mbck: while one probe is in flight the second caller is refused
    /// WITHOUT touching the inner service — one probe means one.
    #[tokio::test]
    async fn halfopen_second_caller_refused_while_probe_in_flight() {
        let manual = ManualClock::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(AtomicBool::new(false));
        let (tx, rx) = tokio::sync::oneshot::channel::<Result<usize, String>>();
        let rx = Arc::new(Mutex::new(Some(rx)));
        let inner = {
            let calls = calls.clone();
            let entered = entered.clone();
            box_send(move |_| {
                let calls = calls.clone();
                let entered = entered.clone();
                let rx = rx.clone();
                Box::pin(async move {
                    let c = calls.fetch_add(1, Ordering::SeqCst);
                    if c == 0 {
                        return Err("down".to_string());
                    }
                    if c == 1 {
                        entered.store(true, Ordering::SeqCst);
                        let recv = rx
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner)
                            .take()
                            .expect("one probe");
                        return recv.await.expect("probe released");
                    }
                    Ok(1usize)
                }) as SendFuture
            })
        };
        let armor =
            SendArmor::with_tuning(inner, Duration::from_secs(5), 4, 1, Duration::from_secs(30))
                .with_clock(manual.clock());
        let out = armor.send(req()).await.expect("armor ok");
        assert_eq!(out, Err("down".to_string()));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let err = armor.send(req()).await.expect_err("open refuses");
        assert_eq!(err, ArmorError::BreakerOpen);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        manual.advance(Duration::from_secs(31));
        let probe = armor.clone();
        let handle = tokio::spawn(async move { probe.send(req()).await });
        let mut spins = 0;
        while !entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
            spins += 1;
            assert!(spins < 10_000, "probe never entered inner");
        }
        let err = armor.send(req()).await.expect_err("second probe refused");
        assert_eq!(err, ArmorError::BreakerOpen);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "exactly one probe touched inner"
        );
        tx.send(Ok(3)).expect("release probe");
        let out = handle.await.expect("probe task").expect("armor ok");
        assert_eq!(out, Ok(3));
        let out = armor.send(req()).await.expect("traffic flows");
        drop(armor);
        assert_eq!(out, Ok(1));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// bd-mbck: the injected clock alone drives recovery — open refuses,
    /// advancing past the reset admits exactly one probe. No sleeps.
    #[tokio::test]
    async fn injected_clock_drives_recovery_without_sleep() {
        let manual = ManualClock::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let armor = SendArmor::with_tuning(
            fake(calls.clone(), move || {
                if count.load(Ordering::SeqCst) == 1 {
                    Err("down".into())
                } else {
                    Ok(1)
                }
            }),
            Duration::from_secs(5),
            4,
            1,
            Duration::from_secs(30),
        )
        .with_clock(manual.clock());
        let out = armor.send(req()).await.expect("armor ok");
        assert_eq!(out, Err("down".to_string()));
        let err = armor.send(req()).await.expect_err("open refuses");
        assert_eq!(err, ArmorError::BreakerOpen);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        manual.advance(Duration::from_secs(31));
        let out = armor.send(req()).await.expect("probe goes through");
        assert_eq!(out, Ok(1), "probe success closes the breaker");
        let out = armor.send(req()).await.expect("traffic flows");
        drop(armor);
        assert_eq!(out, Ok(1));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
