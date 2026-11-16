// governed-by: ADR-0004
#![forbid(unsafe_code)]

//! Mention coalescing (bd-bzu, ADR-0004 Ship 1): batch N @mentions to one
//! assignee within a short window into ONE coalesced job, instead of N
//! separate kind-43001 dispatches that burn the agent's context window.
//!
//! This is the single highest-leverage agent-automation efficiency win from
//! the Buzz study — `buzz-acp`'s `queue.rs` (2,565 LOC) batches N queued
//! events per channel into one `session/prompt`. toron's `dispatch_job`
//! otherwise creates one kind-43001 per @mention; an @mention burst to one
//! agent = N prompts.
//!
//! ## Design (ADR-0004 invariants 1, 5)
//!
//! - **Per-assignee windows.** Each assignee (`agent` name) has its own
//!   coalesce buffer. A burst to agent A and a burst to agent B coalesce
//!   independently — no cross-assignee coupling.
//! - **Configurable window** (default 2s, env `TORON_COALESCE_WINDOW_MS`).
//!   Mentions arriving within `[t0, t0 + window)` of the FIRST mention in a
//!   buffer coalesce into one job.
//! - **Immediate-flush sub-window** (default 500ms, env
//!   `TORON_COALESCE_SUBWINDOW_MS`). The common single-mention case dispatches
//!   at first mention + sub-window, NOT at full window expiry. Only a true
//!   burst (new mentions keep arriving) extends the buffer up to the window
//!   cap. This is the "opt-in-safe" rule: lone mentions pay no full-window
//!   latency.
//! - **Hard cap** (`max_buffer`). Flush at window expiry regardless of buffer
//!   size — no infinite coalesce.
//! - **Order preserved.** The coalesced spec carries an ordered mention list;
//!   arrival order is retained.
//! - **Dedup.** Identical mentions (same `parent` + `spec`) within one window
//!   collapse to one entry.
//! - **Pure + testable.** The coalescer itself is sync, allocation-only, no
//!   I/O, no event-building. Callers (the dispatch ingress) flush buffers and
//!   build the kind-43001 from the [`Coalesced`] payload. The kind-43001 wire
//!   shape is unchanged (a coalesced job is a valid 43001 with a richer
//!   `spec`, not a new kind — ADR-0004 invariant 2).
//!
//! ## Where it sits
//!
//! ```text
//!   @mention arrives
//!     ↓
//!   MentionCoalescer::submit(mention)   ─── per-assignee buffer
//!     ↓ (on window/sub-window/cap flush)
//!   Coalesced { assignee, mentions[], first_arrival }
//!     ↓
//!   caller builds ONE kind-43001 (JobRequest with coalesced spec)
//!     ↓
//!   DispatchPool::enqueue(QueuedJob { … })   (unchanged)
//! ```
//!
//! The coalescer is deliberately decoupled from `DispatchPool`: a caller MAY
//! bypass it (single-mention direct path), and tests can exercise the buffer
//! logic without a bus/event-builder.

use std::collections::VecDeque;
use std::time::Duration;

/// Default coalesce window (ADR-0004: 2s). Env `TORON_COALESCE_WINDOW_MS`.
pub const DEFAULT_WINDOW: Duration = Duration::from_millis(2_000);

/// Default immediate-flush sub-window (ADR-0004: 500ms). Env
/// `TORON_COALESCE_SUBWINDOW_MS`. A buffer whose first mention is older than
/// this AND has seen no new mention is flushed early (lone-mention fast path).
pub const DEFAULT_SUBWINDOW: Duration = Duration::from_millis(500);

/// Hard cap on a single coalesced buffer (defensive — bounds memory + ensures
/// a burst can't coalesce forever). Default 64.
pub const DEFAULT_MAX_BUFFER: usize = 64;

/// Read the coalesce window from `TORON_COALESCE_WINDOW_MS` (millis), falling
/// back to [`DEFAULT_WINDOW`]. Unset/empty/unparseable → default (no panic).
pub fn window_from_env() -> Duration {
    env_millis("TORON_COALESCE_WINDOW_MS", DEFAULT_WINDOW)
}

/// Read the immediate-flush sub-window from `TORON_COALESCE_SUBWINDOW_MS`.
pub fn subwindow_from_env() -> Duration {
    env_millis("TORON_COALESCE_SUBWINDOW_MS", DEFAULT_SUBWINDOW)
}

/// Parse a `TORON_*_MS`-style value: positive integer millis → that duration,
/// else `default`. Pure — tests exercise it without env access (`set_var` is
/// unsafe in edition 2024; env-dependent cases live in `tests/coalesce_env.rs`).
fn parse_millis(s: &str, default: Duration) -> Duration {
    s.trim()
        .parse::<u64>()
        .ok()
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
        .unwrap_or(default)
}

fn env_millis(var: &str, default: Duration) -> Duration {
    std::env::var(var)
        .ok()
        .map(|s| parse_millis(&s, default))
        .unwrap_or(default)
}

/// One inbound @mention (pre-coalesce). The `parent` is the triggering event
/// id (kind-9/14); `spec` is the free-form job-spec fragment the mention
/// carries (e.g. `{"text": "fix the flaky test"}`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct InboundMention {
    /// Target agent name (the @mentioned assignee).
    pub assignee: String,
    /// Triggering event id (becomes the `e` tag parent on the coalesced job).
    pub parent: String,
    /// Free-form spec fragment from this mention.
    #[serde(default)]
    pub spec: serde_json::Value,
    /// Run this mention belongs to (workflow dispatch, bd-6setu.2). `None` on
    /// a hand mention: nothing set it, and nothing may invent it.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Node path inside the run (dispatch step). `None` when no run set it.
    #[serde(default)]
    pub node_path: Option<String>,
}

/// The coalesce key: (assignee, run, node). Two nodes of ONE run for one
/// assignee are two batches, so each becomes its own kind-43001; merging them
/// would hand a coder two steps' work as one job.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CoalesceKey {
    pub assignee: String,
    pub run_id: Option<String>,
    pub node_path: Option<String>,
}

impl CoalesceKey {
    /// The key of a mention that carries no run scope (a hand mention).
    #[must_use]
    pub fn assignee_only(assignee: impl Into<String>) -> Self {
        Self {
            assignee: assignee.into(),
            run_id: None,
            node_path: None,
        }
    }

    /// The key a batch was flushed from.
    #[must_use]
    pub fn of(coalesced: &Coalesced) -> Self {
        Self {
            assignee: coalesced.assignee.clone(),
            run_id: coalesced.run_id.clone(),
            node_path: coalesced.node_path.clone(),
        }
    }
}

impl From<&str> for CoalesceKey {
    fn from(assignee: &str) -> Self {
        Self::assignee_only(assignee)
    }
}

impl From<&String> for CoalesceKey {
    fn from(assignee: &String) -> Self {
        Self::assignee_only(assignee.as_str())
    }
}

impl From<&Coalesced> for CoalesceKey {
    fn from(coalesced: &Coalesced) -> Self {
        Self::of(coalesced)
    }
}

/// A coalesced batch ready to become ONE kind-43001. The caller builds the
/// `JobRequest` from this (the coalesced `spec` is `{"mentions": [...]}` or a
/// caller-chosen merge — see [`Coalesced::merged_spec`]).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Coalesced {
    /// Target agent name.
    pub assignee: String,
    /// Ordered, deduped mentions (arrival order preserved).
    pub mentions: Vec<InboundMention>,
    /// Arrival time of the FIRST mention in the batch (monotonic clock of the
    /// coalescer; used by the caller for the kind-43001 `created_at`).
    pub first_arrival_ms: u64,
    /// Run the batch belongs to, copied from its key (bd-6setu.2).
    #[serde(default)]
    pub run_id: Option<String>,
    /// Node path inside the run, copied from its key.
    #[serde(default)]
    pub node_path: Option<String>,
}

impl Coalesced {
    /// The coalesced count (mentions collapsed into this one job).
    pub const fn count(&self) -> usize {
        self.mentions.len()
    }

    /// A merged spec suitable for the kind-43001 `JobRequest.spec`:
    /// `{"coalesced": true, "mentions": [...]}`. Callers that prefer a custom
    /// merge (e.g. concatenating `text` fields) can ignore this and build
    /// their own from `self.mentions`.
    pub fn merged_spec(&self) -> serde_json::Value {
        serde_json::json!({
            "coalesced": true,
            "count": self.mentions.len(),
            "mentions": self.mentions,
        })
    }

    /// The parent event id for the `e` tag. Uses the FIRST mention's parent
    /// (the triggering event that opened the window) so the coalesced job
    /// links back to the burst's origin.
    pub fn parent(&self) -> &str {
        self.mentions
            .first()
            .map(|m| m.parent.as_str())
            .unwrap_or("")
    }

    /// All mention parent ids in this batch (arrival order, de-duped).
    /// Delivery-cursor peek/mark must cover every id — not only [`Self::parent`] —
    /// so a later batch whose first parent is already delivered still delivers
    /// undelivered siblings (GS-062 multi-parent cutover).
    pub fn parent_ids(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut seen = std::collections::BTreeSet::new();
        for m in &self.mentions {
            if m.parent.is_empty() {
                continue;
            }
            if seen.insert(m.parent.clone()) {
                out.push(m.parent.clone());
            }
        }
        out
    }

    /// GS-062 PARTIAL-KEEP-FULL-SPEC: rebuild a batch containing only mentions
    /// whose parent is in `keep` (reserved undelivered ids). Used when the
    /// delivery cursor skips already-delivered parents so the published
    /// kind-43001 `merged_spec` does not re-include SkipAlreadyDelivered
    /// parents.
    pub fn retain_parents(&self, keep: &[String]) -> Self {
        let keep_set: std::collections::BTreeSet<&str> = keep.iter().map(String::as_str).collect();
        Self {
            assignee: self.assignee.clone(),
            mentions: self
                .mentions
                .iter()
                .filter(|m| keep_set.contains(m.parent.as_str()))
                .cloned()
                .collect(),
            first_arrival_ms: self.first_arrival_ms,
            run_id: self.run_id.clone(),
            node_path: self.node_path.clone(),
        }
    }
}

/// What [`MentionCoalescer::submit`] reports back to the caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoalesceDecision {
    /// The mention was buffered (window open). The caller MAY drive a timer
    /// and call [`MentionCoalescer::flush_if_ready`] / [`flush_all`] later.
    Buffered {
        /// Current buffer depth for this assignee.
        buffered: usize,
    },
    /// The buffer hit the hard cap and flushed NOW. Build + dispatch one
    /// kind-43001 from the [`Coalesced`].
    FlushNow(Coalesced),
}

/// What [`MentionCoalescer::flush_if_ready`] / [`tick`] report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Nothing was ready to flush (no buffer, or window not yet elapsed).
    NotReady,
    /// A buffer flushed. Build + dispatch one kind-43001.
    Flushed(Coalesced),
}

/// Per-assignee coalesce buffer state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Buffer {
    /// Ordered, deduped mentions (arrival order).
    mentions: VecDeque<InboundMention>,
    /// `first_arrival_ms` of the buffer's opening mention (monotonic ms).
    first_arrival_ms: u64,
    /// `last_arrival_ms` of the most-recent mention (drives the sub-window
    /// immediate-flush fast path).
    last_arrival_ms: u64,
    /// Earliest wall-clock ms at which a requeue backoff permits a flush.
    /// Arrival timestamps stay truthful; backoff lives here.
    ready_at_ms: u64,
    /// A zero-delay requeue that must flush on the next tick.
    immediate: bool,
}

impl Buffer {
    const fn new(now_ms: u64) -> Self {
        Self {
            mentions: VecDeque::new(),
            first_arrival_ms: now_ms,
            last_arrival_ms: now_ms,
            ready_at_ms: 0,
            immediate: false,
        }
    }

    fn push_dedup(&mut self, m: InboundMention) -> bool {
        let dup = self
            .mentions
            .iter()
            .any(|existing| existing.parent == m.parent && existing.spec == m.spec);
        if dup {
            return false;
        }
        self.mentions.push_back(m);
        true
    }
}

/// The mention coalescer. Per-assignee windows, sync + allocation-only.
///
/// Time is injected (`now_ms`) so the logic is deterministic in tests; the
/// caller reads a monotonic clock (e.g. `tokio::time::Instant` since epoch, or
/// a virtual clock in tests) and passes it in.
/// Max Transient (Transport / acquire) requeues per assignee before the
/// coalescer stops restoring the burst (GS-062 TRANSPORT-REQUEUE-HOT-SPIN).
pub const REQUEUE_MAX_ATTEMPTS: u32 = 8;
/// Base delay for Transient backoff (attempt 1+); attempt 0 stays immediate.
pub const REQUEUE_BACKOFF_BASE_MS: u64 = 250;
/// Cap for Transient exponential backoff.
pub const REQUEUE_BACKOFF_CAP_MS: u64 = 8_000;

/// How [`MentionCoalescer::requeue`] schedules the next flush.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueSchedule {
    /// Fresh coalesce window at `now_ms` (InFlight — peer still publishing).
    /// Does **not** burn the Transient retry budget. `flush_ready=false` OK.
    FreshWindow,
    /// Transport / acquire Err — exponential backoff + retry budget.
    /// Honors readiness even when merging into a non-empty buffer
    /// (REQUEUE-FLUSH-READY-LOST-ON-CONCURRENT-SUBMIT).
    Transient,
}

/// Result of [`MentionCoalescer::requeue`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueOutcome {
    /// Burst restored; next flush eligible after `delay_ms` from `now_ms`.
    Queued { attempt: u32, delay_ms: u64 },
    /// Retry budget exhausted — caller must ProvenFailed **without** force-retry
    /// requeue. The burst is parked on the durable DLQ (not live-buffer restore,
    /// which would hot-spin; not silent drop — GS-062 T2).
    Exhausted,
}

/// Why a coalesced burst was parked on the durable DLQ (GS-062 T2/T3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqReason {
    /// Transient Transport/acquire retry budget exhausted.
    Exhausted,
    /// No job signer configured (terminal; align timer with FlushNow ProvenFailed).
    NoSigner,
}

/// One durable-failed coalesced burst (not silently dropped).
#[derive(Debug, Clone, PartialEq)]
pub struct DlqEntry {
    pub reason: DlqReason,
    pub coalesced: Coalesced,
}

/// Deterministic Transient backoff delay for `attempt` (0-indexed count
/// **before** this requeue). Attempt 0 → immediate (0ms); then
/// `base * 2^(n-1)` capped, plus small deterministic jitter.
pub fn transient_backoff_delay_ms(assignee: &str, attempt: u32) -> u64 {
    if attempt == 0 {
        return 0;
    }
    let shift = (attempt - 1).min(5);
    let base = REQUEUE_BACKOFF_BASE_MS.saturating_mul(1u64 << shift);
    let capped = base.min(REQUEUE_BACKOFF_CAP_MS);
    let jitter = transient_jitter_ms(assignee, attempt, capped);
    capped
        .saturating_add(jitter)
        .min(REQUEUE_BACKOFF_CAP_MS.saturating_mul(2))
}

fn transient_jitter_ms(assignee: &str, attempt: u32, delay: u64) -> u64 {
    if delay == 0 {
        return 0;
    }
    let mut h = 0u64;
    for b in assignee.as_bytes() {
        h = h.wrapping_mul(31).wrapping_add(u64::from(*b));
    }
    h = h.wrapping_add(u64::from(attempt)).wrapping_mul(0x9E37_79B9);
    h % (delay / 4 + 1)
}

pub struct MentionCoalescer {
    window: Duration,
    subwindow: Duration,
    max_buffer: usize,
    buffers: std::collections::BTreeMap<CoalesceKey, Buffer>,
    /// Per-assignee Transient requeue attempts (cleared on successful publish).
    retry_attempts: std::collections::BTreeMap<CoalesceKey, u32>,
    /// Durable fail park (Exhausted / NoSigner) — not silent drop (GS-062 T2/T3).
    dlq: Vec<DlqEntry>,
}

impl MentionCoalescer {
    /// New coalescer with explicit timing.
    pub fn new(window: Duration, subwindow: Duration, max_buffer: usize) -> Self {
        Self {
            window,
            subwindow,
            max_buffer,
            buffers: std::collections::BTreeMap::new(),
            retry_attempts: std::collections::BTreeMap::new(),
            dlq: Vec::new(),
        }
    }

    /// New coalescer from env (`TORON_COALESCE_WINDOW_MS`,
    /// `TORON_COALESCE_SUBWINDOW_MS`) + default max-buffer.
    pub fn from_env() -> Self {
        Self::new(window_from_env(), subwindow_from_env(), DEFAULT_MAX_BUFFER)
    }

    /// New coalescer with ADR-0004 defaults (2s window, 500ms sub-window,
    /// 64-entry cap).
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_WINDOW, DEFAULT_SUBWINDOW, DEFAULT_MAX_BUFFER)
    }

    /// Submit one inbound mention. Returns whether it was buffered or
    /// triggered an immediate cap-flush.
    ///
    /// `now_ms` is a monotonic-millis clock the caller supplies (deterministic
    /// in tests).
    pub fn submit(&mut self, mention: InboundMention, now_ms: u64) -> CoalesceDecision {
        let key = CoalesceKey {
            assignee: mention.assignee.clone(),
            run_id: mention.run_id.clone(),
            node_path: mention.node_path.clone(),
        };
        let buf = self
            .buffers
            .entry(key.clone())
            .or_insert_with(|| Buffer::new(now_ms));
        if buf.mentions.is_empty() {
            buf.first_arrival_ms = now_ms;
        }
        buf.last_arrival_ms = now_ms.max(buf.last_arrival_ms);
        buf.push_dedup(mention);

        if buf.mentions.len() >= self.max_buffer {
            let coalesced = self.take_buffer(&key).expect("just-inserted buffer");
            return CoalesceDecision::FlushNow(coalesced);
        }
        CoalesceDecision::Buffered {
            buffered: buf.mentions.len(),
        }
    }

    /// Flush an assignee's buffer IFF the window has elapsed OR the sub-window
    /// immediate-flush fast path has fired (buffer non-empty, no new mention
    /// within `subwindow`). Returns `NotReady` otherwise.
    ///
    /// The caller drives this on a timer (e.g. every 100ms) per assignee, or
    /// via [`Self::tick`] to check all assignees at once.
    pub fn flush_if_ready(&mut self, assignee: &str, now_ms: u64) -> Vec<Coalesced> {
        let ready: Vec<CoalesceKey> = self
            .buffers
            .iter()
            .filter(|(key, buf)| key.assignee == assignee && self.ready(buf, now_ms))
            .map(|(key, _)| key.clone())
            .collect();
        ready
            .into_iter()
            .filter_map(|key| self.take_buffer(&key))
            .collect()
    }

    /// Is this buffer due? A zero-delay requeue (`immediate`), the window, or
    /// the sub-window fast path, and never before a backoff's `ready_at_ms`.
    fn ready(&self, buf: &Buffer, now_ms: u64) -> bool {
        if buf.mentions.is_empty() {
            return false;
        }
        let backoff_ready = now_ms >= buf.ready_at_ms;
        let window_elapsed = backoff_ready
            && now_ms.saturating_sub(buf.first_arrival_ms) >= self.window.as_millis() as u64;
        let subwindow_elapsed = backoff_ready
            && now_ms.saturating_sub(buf.last_arrival_ms) >= self.subwindow.as_millis() as u64;
        buf.immediate || window_elapsed || subwindow_elapsed
    }

    /// Check ALL assignees for ready flushes (caller-driven tick). Returns the
    /// flushed batches in assignee-name-sorted order (deterministic).
    pub fn tick(&mut self, now_ms: u64) -> Vec<Coalesced> {
        let ready: Vec<CoalesceKey> = self
            .buffers
            .iter()
            .filter(|(_, buf)| self.ready(buf, now_ms))
            .map(|(key, _)| key.clone())
            .collect();
        ready
            .into_iter()
            .filter_map(|key| self.take_buffer(&key))
            .collect()
    }

    /// Force-flush every batch of one assignee now (e.g. on shutdown or an
    /// operator nudge). Key-sorted; empty when nothing was buffered.
    pub fn force_flush(&mut self, assignee: &str) -> Vec<Coalesced> {
        let keys: Vec<CoalesceKey> = self
            .buffers
            .keys()
            .filter(|key| key.assignee == assignee)
            .cloned()
            .collect();
        keys.into_iter()
            .filter_map(|key| self.take_buffer(&key))
            .collect()
    }

    /// Force-flush ALL buffers now (e.g. on shutdown). Key-sorted order.
    pub fn force_flush_all(&mut self) -> Vec<Coalesced> {
        let keys: Vec<CoalesceKey> = self.buffers.keys().cloned().collect();
        keys.into_iter()
            .filter_map(|key| self.take_buffer(&key))
            .collect()
    }

    /// Clear Transient retry counter after a successful publish. Takes the
    /// batch's key (or a bare assignee for the key-less hand path), so one
    /// node's success never wipes another node's budget.
    pub fn clear_retry(&mut self, key: impl Into<CoalesceKey>) {
        self.retry_attempts.remove(&key.into());
    }

    /// Park a coalesced burst on the durable DLQ (timer NoSigner / host
    /// terminal paths). Does not touch the live buffer or retry map.
    pub fn park_dlq(&mut self, coalesced: Coalesced, reason: DlqReason) {
        self.dlq.push(DlqEntry { reason, coalesced });
    }

    /// Number of DLQ entries (observable for tests / ops).
    pub const fn dlq_len(&self) -> usize {
        self.dlq.len()
    }

    /// Drain the durable DLQ (oldest first).
    pub fn take_dlq(&mut self) -> Vec<DlqEntry> {
        std::mem::take(&mut self.dlq)
    }

    /// DLQ entries for one assignee (borrowed; tests / ops).
    pub fn dlq_for(&self, assignee: &str) -> Vec<&DlqEntry> {
        self.dlq
            .iter()
            .filter(|e| e.coalesced.assignee == assignee)
            .collect()
    }

    /// Current Transient attempt count for a key (0 if none).
    pub fn retry_attempts(&self, key: impl Into<CoalesceKey>) -> u32 {
        self.retry_attempts.get(&key.into()).copied().unwrap_or(0)
    }

    /// Re-insert a previously flushed batch so it is not silently dropped
    /// (GS-062: dispatch acquire deferred / in-flight delivery cursor /
    /// Transport retry).
    ///
    /// - [`RequeueSchedule::FreshWindow`]: opens a fresh window at `now_ms`
    ///   when the buffer was empty; leaves existing non-empty timing alone.
    ///   Does not burn the Transient budget (InFlight path).
    /// - [`RequeueSchedule::Transient`]: exponential backoff + retry budget.
    ///   Schedules readiness even when merging into a non-empty buffer so a
    ///   concurrent submit during publish await cannot erase immediate-retry
    ///   intent (REQUEUE-FLUSH-READY-LOST-ON-CONCURRENT-SUBMIT).
    pub fn requeue(
        &mut self,
        coalesced: Coalesced,
        now_ms: u64,
        schedule: RequeueSchedule,
    ) -> RequeueOutcome {
        let key = CoalesceKey::of(&coalesced);
        let (delay_ms, attempt) = match schedule {
            RequeueSchedule::FreshWindow => (None, 0u32),
            RequeueSchedule::Transient => {
                let attempt = self.retry_attempts(key.clone());
                if attempt >= REQUEUE_MAX_ATTEMPTS {
                    self.retry_attempts.remove(&key);
                    self.dlq.push(DlqEntry {
                        reason: DlqReason::Exhausted,
                        coalesced,
                    });
                    return RequeueOutcome::Exhausted;
                }
                let delay = transient_backoff_delay_ms(key.assignee.as_str(), attempt);
                self.retry_attempts
                    .insert(key.clone(), attempt.saturating_add(1));
                (Some(delay), attempt.saturating_add(1))
            }
        };

        let buf = self
            .buffers
            .entry(key.clone())
            .or_insert_with(|| Buffer::new(now_ms));
        match delay_ms {
            Some(delay) => {
                buf.ready_at_ms = now_ms.saturating_add(delay);
                buf.immediate = delay == 0;
            }
            None => {
                buf.ready_at_ms = now_ms;
                buf.immediate = false;
                if buf.mentions.is_empty() {
                    buf.first_arrival_ms = now_ms;
                    buf.last_arrival_ms = now_ms;
                }
            }
        }
        for m in coalesced.mentions {
            let _ = buf.push_dedup(m);
        }
        RequeueOutcome::Queued {
            attempt,
            delay_ms: delay_ms.unwrap_or(0),
        }
    }

    /// Buffered mentions for an assignee across all of its keys (0 if none).
    pub fn buffered_for(&self, assignee: &str) -> usize {
        self.buffers
            .iter()
            .filter(|(key, _)| key.assignee == assignee)
            .map(|(_, b)| b.mentions.len())
            .sum()
    }

    /// Number of open (non-empty) buffers, one per key.
    pub fn open_buffers(&self) -> usize {
        self.buffers
            .values()
            .filter(|b| !b.mentions.is_empty())
            .count()
    }

    /// Drain + coalesce one key's buffer into a batch that names its scope.
    fn take_buffer(&mut self, key: &CoalesceKey) -> Option<Coalesced> {
        let buf = self.buffers.get_mut(key)?;
        if buf.mentions.is_empty() {
            return None;
        }
        let mentions: Vec<InboundMention> = buf.mentions.drain(..).collect();
        let first_arrival_ms = buf.first_arrival_ms;
        buf.first_arrival_ms = 0;
        buf.last_arrival_ms = 0;
        buf.ready_at_ms = 0;
        buf.immediate = false;
        Some(Coalesced {
            assignee: key.assignee.clone(),
            mentions,
            first_arrival_ms,
            run_id: key.run_id.clone(),
            node_path: key.node_path.clone(),
        })
    }
}

impl Default for MentionCoalescer {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(assignee: &str, parent: &str, text: &str) -> InboundMention {
        InboundMention {
            assignee: assignee.into(),
            parent: parent.into(),
            spec: serde_json::json!({ "text": text }),
            run_id: None,
            node_path: None,
        }
    }

    /// GS-062: requeue restores a flushed burst (acquire-drop / in-flight retry).
    #[test]
    fn requeue_restores_burst_for_retry() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        c.submit(m("oc", "e1", "hi"), 0);
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
        assert_eq!(c.buffered_for("oc"), 0);
        let q = c.requeue(batch, 1_000, RequeueSchedule::Transient);
        assert!(
            matches!(q, RequeueOutcome::Queued { delay_ms: 0, .. }),
            "{q:?}"
        );
        assert_eq!(c.buffered_for("oc"), 1);
        let again = c.tick(1_000);
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].parent(), "e1");
    }

    /// GS-062 REQUEUE-EVENT-STARVATION: InFlight-style requeue (flush_ready=false)
    /// must still flush once the sub-window elapses — a timer tick (not only a
    /// new bus event) can advance `now_ms` into flush readiness.
    #[test]
    fn requeue_inflight_style_flushes_after_subwindow() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        c.submit(
            InboundMention {
                run_id: None,
                node_path: None,
                assignee: "oc".into(),
                parent: "p1".into(),
                spec: serde_json::json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
        let q = c.requeue(batch, 1_000, RequeueSchedule::FreshWindow);
        assert!(matches!(q, RequeueOutcome::Queued { .. }), "{q:?}");
        assert_eq!(c.buffered_for("oc"), 1);
        assert!(
            c.tick(1_000).is_empty(),
            "immediate tick must not flush (window just opened)"
        );
        let flushed = c.tick(1_000 + 500);
        assert_eq!(
            flushed.len(),
            1,
            "subwindow elapsed → flush without a new submit/event"
        );
        assert_eq!(flushed[0].parent(), "p1");
    }

    /// GS-062 TRANSPORT-REQUEUE-HOT-SPIN: Transient backoff grows; budget exhausts.
    #[test]
    fn transient_requeue_backoff_and_budget() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        c.submit(m("oc", "e1", "hi"), 0);
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
        let q0 = c.requeue(batch, 1_000, RequeueSchedule::Transient);
        assert_eq!(
            q0,
            RequeueOutcome::Queued {
                attempt: 1,
                delay_ms: 0
            }
        );
        assert_eq!(c.tick(1_000).len(), 1);
        let mut last_delay = 0u64;
        for i in 1..REQUEUE_MAX_ATTEMPTS {
            let now = 2_000 + u64::from(i) * 10;
            c.submit(m("oc", "e1", "hi"), 0);
            let batch = c.force_flush("oc").into_iter().next().expect("flushed");
            let q = c.requeue(batch, now, RequeueSchedule::Transient);
            match q {
                RequeueOutcome::Queued { delay_ms, attempt } => {
                    assert_eq!(attempt, i + 1);
                    if last_delay < REQUEUE_BACKOFF_CAP_MS {
                        assert!(
                            delay_ms + 1 >= last_delay,
                            "backoff must grow before cap: {delay_ms} << {last_delay}"
                        );
                    }
                    last_delay = delay_ms;
                    assert!(
                        c.tick(now).is_empty(),
                        "attempt {attempt} delay {delay_ms} must defer at now"
                    );
                    assert_eq!(c.tick(now + delay_ms).len(), 1, "flush after backoff");
                }
                RequeueOutcome::Exhausted => panic!("exhausted too early at i={i}"),
            }
        }
        c.submit(m("oc", "e1", "hi"), 0);
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
        assert_eq!(
            c.requeue(batch, 9_000, RequeueSchedule::Transient),
            RequeueOutcome::Exhausted
        );
        assert_eq!(
            c.buffered_for("oc"),
            0,
            "Exhausted must not live-buffer restore"
        );
        assert_eq!(c.dlq_len(), 1, "Exhausted must park whole batch on DLQ");
        assert_eq!(
            c.dlq_for("oc")[0].reason,
            DlqReason::Exhausted,
            "DLQ reason Exhausted"
        );
        assert_eq!(
            c.retry_attempts("oc"),
            0,
            "Exhausted must clear retry map (no poison for later Transient)"
        );
        c.submit(m("oc", "e2", "again"), 10_000);
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
        assert!(
            matches!(
                c.requeue(batch, 10_000, RequeueSchedule::Transient),
                RequeueOutcome::Queued {
                    attempt: 1,
                    delay_ms: 0
                }
            ),
            "post-Exhausted Transient must not poison-Exhaust"
        );
    }

    /// GS-062 REQUEUE-FLUSH-READY-LOST-ON-CONCURRENT-SUBMIT: Transient schedule
    /// survives merge into a non-empty buffer (concurrent submit during publish).
    #[test]
    fn requeue_transient_honors_schedule_on_nonempty_merge() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        c.submit(m("oc", "concurrent", "new"), 1_000);
        assert_eq!(c.buffered_for("oc"), 1);
        let failed = Coalesced {
            assignee: "oc".into(),
            mentions: vec![m("oc", "failed", "retry")],
            first_arrival_ms: 0,
            run_id: None,
            node_path: None,
        };
        let q = c.requeue(failed, 1_000, RequeueSchedule::Transient);
        assert!(
            matches!(q, RequeueOutcome::Queued { delay_ms: 0, .. }),
            "{q:?}"
        );
        assert_eq!(c.buffered_for("oc"), 2, "merge both mentions");
        let flushed = c.tick(1_000);
        assert_eq!(flushed.len(), 1);
        let parents: Vec<_> = flushed[0]
            .mentions
            .iter()
            .map(|x| x.parent.as_str())
            .collect();
        assert!(parents.contains(&"concurrent"), "{parents:?}");
        assert!(parents.contains(&"failed"), "{parents:?}");
    }

    /// GS-062 TRANSPORT backoff clobber: submit during Transient backoff must
    /// NOT collapse `last_arrival_ms` and fire the sub-window early.
    #[test]
    fn submit_during_transient_backoff_does_not_fire_early() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(100), 64);
        c.submit(m("oc", "e1", "hi"), 0);
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
        assert!(matches!(
            c.requeue(batch, 1_000, RequeueSchedule::Transient),
            RequeueOutcome::Queued { delay_ms: 0, .. }
        ));
        let batch = c.tick(1_000).pop().expect("immediate flush");
        let q = c.requeue(batch, 2_000, RequeueSchedule::Transient);
        let delay_ms = match q {
            RequeueOutcome::Queued { delay_ms, attempt } => {
                assert_eq!(attempt, 2);
                assert!(
                    delay_ms > 100,
                    "need backoff > subwindow to expose clobber, got {delay_ms}"
                );
                delay_ms
            }
            other => panic!("expected Queued backoff, got {other:?}"),
        };
        c.submit(m("oc", "live", "during-backoff"), 2_100);
        assert_eq!(
            c.buffered_for("oc"),
            2,
            "merge concurrent into backoff buffer"
        );
        assert!(
            c.tick(2_100 + 100).is_empty(),
            "subwindow after concurrent submit must NOT flush before backoff ready_at"
        );
        assert!(
            c.tick(2_000 + delay_ms - 1).is_empty(),
            "must still wait for full Transient backoff"
        );
        let flushed = c.tick(2_000 + delay_ms);
        assert_eq!(flushed.len(), 1, "flush only at scheduled ready_at");
        let parents: Vec<_> = flushed[0]
            .mentions
            .iter()
            .map(|x| x.parent.as_str())
            .collect();
        assert!(parents.contains(&"e1"), "{parents:?}");
        assert!(parents.contains(&"live"), "{parents:?}");
    }

    /// GS-062 PARTIAL-KEEP-FULL-SPEC: retain_parents drops SkipAlreadyDelivered parents.
    #[test]
    fn retain_parents_filters_merged_spec() {
        let batch = Coalesced {
            assignee: "oc".into(),
            run_id: None,
            node_path: None,
            mentions: vec![
                InboundMention {
                    run_id: None,
                    node_path: None,
                    assignee: "oc".into(),
                    parent: "already".into(),
                    spec: serde_json::json!({"n": 1}),
                },
                InboundMention {
                    run_id: None,
                    node_path: None,
                    assignee: "oc".into(),
                    parent: "keep-me".into(),
                    spec: serde_json::json!({"n": 2}),
                },
            ],
            first_arrival_ms: 0,
        };
        let kept = batch.retain_parents(&["keep-me".into()]);
        assert_eq!(kept.parent_ids(), vec!["keep-me".to_string()]);
        let spec = kept.merged_spec();
        assert_eq!(spec["count"], 1);
        let mentions = spec["mentions"].as_array().expect("mentions");
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0]["parent"], "keep-me");
    }

    /// ADR-0004 verification: 20 mentions to one assignee within the window →
    /// 1 coalesced job (not 20).
    #[test]
    fn burst_coalesces_to_one_job() {
        let mut c = MentionCoalescer::new(DEFAULT_WINDOW, DEFAULT_SUBWINDOW, DEFAULT_MAX_BUFFER);
        for i in 0..20 {
            let dec = c.submit(m("oc", "evt-a", &format!("mention {i}")), 1_000 + i * 10);
            assert!(
                matches!(dec, CoalesceDecision::Buffered { .. }),
                "mention {i} should buffer, got {dec:?}"
            );
        }
        let flushed = c.force_flush_all();
        assert_eq!(flushed.len(), 1, "one assignee → one batch");
        let batch = &flushed[0];
        assert_eq!(batch.assignee, "oc");
        assert_eq!(batch.count(), 20, "all 20 mentions coalesced");
        assert_eq!(batch.mentions[0].spec["text"], "mention 0");
        assert_eq!(batch.mentions[19].spec["text"], "mention 19");
    }

    /// ADR-0004 invariant 5: a LONE mention dispatches at first-mention +
    /// sub-window, NOT at full window. (Common case pays no 2s latency.)
    #[test]
    fn lone_mention_flushes_at_subwindow_not_full_window() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        c.submit(m("oc", "e1", "hi"), 0);
        assert!(
            c.flush_if_ready("oc", 400).is_empty(),
            "before the sub-window nothing flushes"
        );
        let out = c.flush_if_ready("oc", 500);
        assert_eq!(out.len(), 1, "sub-window flush returns the ready batch");
        assert_eq!(out[0].count(), 1);
    }

    /// A burst that KEEPS arriving stays buffered up to the full window (the
    /// sub-window fast path resets on each new mention).
    #[test]
    fn ongoing_burst_stays_buffered_until_full_window() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        for i in 0..5 {
            c.submit(m("oc", "e", &format!("m{i}")), i * 300);
        }
        assert!(
            c.flush_if_ready("oc", 1_200).is_empty(),
            "a live burst stays buffered"
        );
        let out = c.flush_if_ready("oc", 1_700);
        assert_eq!(out.len(), 1, "expected one flush at t=1700");
        assert_eq!(out[0].count(), 5);
    }

    /// Hard cap flushes immediately (no infinite coalesce).
    #[test]
    fn hard_cap_flushes_now() {
        let mut c = MentionCoalescer::new(DEFAULT_WINDOW, DEFAULT_SUBWINDOW, 3);
        let _ = c.submit(m("oc", "e1", "a"), 0);
        let _ = c.submit(m("oc", "e2", "b"), 10);
        let dec = c.submit(m("oc", "e3", "c"), 20);
        match dec {
            CoalesceDecision::FlushNow(batch) => {
                assert_eq!(batch.count(), 3);
                assert_eq!(batch.assignee, "oc");
            }
            other => panic!("expected FlushNow at cap, got {other:?}"),
        }
        assert_eq!(c.buffered_for("oc"), 0);
    }

    /// Dedup: identical (parent, spec) within one window collapses.
    #[test]
    fn identical_mentions_dedup() {
        let mut c = MentionCoalescer::with_defaults();
        let _ = c.submit(m("oc", "e1", "same"), 0);
        let dec = c.submit(m("oc", "e1", "same"), 10);
        match dec {
            CoalesceDecision::Buffered { buffered } => assert_eq!(buffered, 1, "dup not added"),
            other => panic!("dup should buffer (not flush), got {other:?}"),
        }
        let batch = c.force_flush("oc").into_iter().next().expect("flush");
        assert_eq!(batch.count(), 1, "deduped to one");
    }

    /// Different assignees coalesce independently.
    #[test]
    fn per_assignee_isolation() {
        let mut c = MentionCoalescer::with_defaults();
        c.submit(m("oc", "e1", "for oc"), 0);
        c.submit(m("gk", "e2", "for gk"), 0);
        c.submit(m("oc", "e3", "for oc 2"), 10);
        assert_eq!(c.buffered_for("oc"), 2);
        assert_eq!(c.buffered_for("gk"), 1);
        let all = c.force_flush_all();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].assignee, "gk");
        assert_eq!(all[0].count(), 1);
        assert_eq!(all[1].assignee, "oc");
        assert_eq!(all[1].count(), 2);
    }

    /// Mentions outside the window (after a flush) start a fresh batch — no
    /// infinite coalesce across windows.
    #[test]
    fn mentions_outside_window_are_separate_jobs() {
        let mut c = MentionCoalescer::new(Duration::from_millis(1_000), DEFAULT_SUBWINDOW, 64);
        c.submit(m("oc", "e1", "first batch"), 0);
        let first = c.force_flush("oc").into_iter().next().expect("first flush");
        assert_eq!(first.count(), 1);
        c.submit(m("oc", "e2", "second batch"), 5_000);
        let second = c
            .force_flush("oc")
            .into_iter()
            .next()
            .expect("second flush");
        assert_eq!(second.count(), 1);
        assert_ne!(first.parent(), second.parent(), "distinct parents");
    }

    /// `tick()` flushes all ready assignees in one call (caller-driven timer).
    #[test]
    fn tick_flushes_all_ready() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(1_000), Duration::from_millis(500), 64);
        c.submit(m("oc", "e1", "a"), 0);
        c.submit(m("gk", "e2", "b"), 0);
        let flushed = c.tick(500);
        assert_eq!(flushed.len(), 2, "both assignees flush");
        assert_eq!(flushed[0].assignee, "gk");
        assert_eq!(flushed[1].assignee, "oc");
    }

    /// merged_spec produces the documented `{"coalesced":true,"count":N,...}`.
    #[test]
    fn merged_spec_shape() {
        let mut c = MentionCoalescer::with_defaults();
        c.submit(m("oc", "e1", "a"), 0);
        c.submit(m("oc", "e2", "b"), 10);
        let batch = c.force_flush("oc").into_iter().next().expect("flush");
        let spec = batch.merged_spec();
        assert_eq!(spec["coalesced"], true);
        assert_eq!(spec["count"], 2);
        assert!(spec["mentions"].is_array());
        assert_eq!(spec["mentions"].as_array().unwrap().len(), 2);
    }

    /// bd-6setu.2: two nodes of ONE run for one assignee are two batches, so
    /// each becomes its own job. Merging them would hand one seat two steps'
    /// work as a single ask.
    #[test]
    fn two_nodes_of_one_run_flush_as_two_batches() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        let run = |node: &str, parent: &str, text: &str| {
            let mut mention = m("coder", parent, text);
            mention.run_id = Some("run-1".into());
            mention.node_path = Some(node.into());
            mention
        };
        c.submit(run("plan", "e1", "plot"), 0);
        c.submit(run("execute", "e2", "build"), 0);
        assert_eq!(
            c.open_buffers(),
            2,
            "one buffer per (assignee, run, node), not one per assignee"
        );
        let batches = c.force_flush("coder");
        assert_eq!(batches.len(), 2, "two nodes of one run are two jobs");
        let mut nodes: Vec<&str> = batches
            .iter()
            .filter_map(|b| b.node_path.as_deref())
            .collect();
        nodes.sort_unstable();
        assert_eq!(nodes, vec!["execute", "plan"]);
        assert!(
            batches.iter().all(|b| b.run_id.as_deref() == Some("run-1")),
            "both batches keep the run"
        );
        assert!(
            batches.iter().all(|b| b.count() == 1),
            "no mention leaks into the other node's batch"
        );
    }
}
