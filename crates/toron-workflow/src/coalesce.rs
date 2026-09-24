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
        let keep_set: std::collections::BTreeSet<&str> =
            keep.iter().map(String::as_str).collect();
        Self {
            assignee: self.assignee.clone(),
            mentions: self
                .mentions
                .iter()
                .filter(|m| keep_set.contains(m.parent.as_str()))
                .cloned()
                .collect(),
            first_arrival_ms: self.first_arrival_ms,
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
}

impl Buffer {
    const fn new(now_ms: u64) -> Self {
        Self {
            mentions: VecDeque::new(),
            first_arrival_ms: now_ms,
            last_arrival_ms: now_ms,
        }
    }

    fn push_dedup(&mut self, m: InboundMention) -> bool {
        // Dedup: identical (parent, spec) already in this buffer → drop.
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
pub struct MentionCoalescer {
    window: Duration,
    subwindow: Duration,
    max_buffer: usize,
    buffers: std::collections::HashMap<String, Buffer>,
}

impl MentionCoalescer {
    /// New coalescer with explicit timing.
    pub fn new(window: Duration, subwindow: Duration, max_buffer: usize) -> Self {
        Self {
            window,
            subwindow,
            max_buffer,
            buffers: std::collections::HashMap::new(),
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
        let assignee = mention.assignee.clone();
        let buf = self
            .buffers
            .entry(assignee.clone())
            .or_insert_with(|| Buffer::new(now_ms));
        // Open the window on the first mention; subsequent mentions slide the
        // last_arrival forward (sub-window fast path) but do NOT extend
        // first_arrival (window is measured from the opening mention).
        if buf.mentions.is_empty() {
            buf.first_arrival_ms = now_ms;
        }
        buf.last_arrival_ms = now_ms;
        buf.push_dedup(mention);

        if buf.mentions.len() >= self.max_buffer {
            // Hard cap → flush now (no infinite coalesce).
            let coalesced = self.take_buffer(&assignee).expect("just-inserted buffer");
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
    pub fn flush_if_ready(&mut self, assignee: &str, now_ms: u64) -> FlushOutcome {
        let Some(buf) = self.buffers.get(assignee) else {
            return FlushOutcome::NotReady;
        };
        if buf.mentions.is_empty() {
            return FlushOutcome::NotReady;
        }
        let window_elapsed =
            now_ms.saturating_sub(buf.first_arrival_ms) >= self.window.as_millis() as u64;
        let subwindow_elapsed =
            now_ms.saturating_sub(buf.last_arrival_ms) >= self.subwindow.as_millis() as u64;
        if window_elapsed || subwindow_elapsed {
            let coalesced = self
                .take_buffer(assignee)
                .expect("non-empty buffer checked above");
            FlushOutcome::Flushed(coalesced)
        } else {
            FlushOutcome::NotReady
        }
    }

    /// Check ALL assignees for ready flushes (caller-driven tick). Returns the
    /// flushed batches in assignee-name-sorted order (deterministic).
    pub fn tick(&mut self, now_ms: u64) -> Vec<Coalesced> {
        let assignees: Vec<String> = self.buffers.keys().cloned().collect();
        let mut ready: Vec<String> = Vec::new();
        for a in &assignees {
            if let Some(buf) = self.buffers.get(a) {
                if buf.mentions.is_empty() {
                    continue;
                }
                let window_elapsed =
                    now_ms.saturating_sub(buf.first_arrival_ms) >= self.window.as_millis() as u64;
                let subwindow_elapsed =
                    now_ms.saturating_sub(buf.last_arrival_ms) >= self.subwindow.as_millis() as u64;
                if window_elapsed || subwindow_elapsed {
                    ready.push(a.clone());
                }
            }
        }
        ready.sort();
        ready
            .into_iter()
            .filter_map(|a| self.take_buffer(&a))
            .collect()
    }

    /// Force-flush a specific assignee's buffer now (e.g. on shutdown or an
    /// operator nudge). Returns `None` if the buffer was empty/absent.
    pub fn force_flush(&mut self, assignee: &str) -> Option<Coalesced> {
        self.take_buffer(assignee)
    }

    /// Force-flush ALL buffers now (e.g. on shutdown). Assignee-sorted order.
    pub fn force_flush_all(&mut self) -> Vec<Coalesced> {
        let mut assignees: Vec<String> = self.buffers.keys().cloned().collect();
        assignees.sort();
        assignees
            .into_iter()
            .filter_map(|a| self.take_buffer(&a))
            .collect()
    }

    /// Re-insert a previously flushed batch so it is not silently dropped
    /// (GS-062: dispatch acquire deferred / in-flight delivery cursor).
    ///
    /// When `flush_ready` is true the window is backdated so the next
    /// [`Self::tick`] / sub-window check can retry immediately; when false a
    /// fresh window opens at `now_ms` (avoids tight requeue spin on SkipInFlight).
    pub fn requeue(&mut self, coalesced: Coalesced, now_ms: u64, flush_ready: bool) {
        let assignee = coalesced.assignee.clone();
        let buf = self
            .buffers
            .entry(assignee)
            .or_insert_with(|| Buffer::new(now_ms));
        if buf.mentions.is_empty() {
            if flush_ready {
                let win = self.window.as_millis() as u64;
                let sub = self.subwindow.as_millis() as u64;
                buf.first_arrival_ms = now_ms.saturating_sub(win);
                buf.last_arrival_ms = now_ms.saturating_sub(sub);
            } else {
                buf.first_arrival_ms = now_ms;
                buf.last_arrival_ms = now_ms;
            }
        }
        for m in coalesced.mentions {
            let _ = buf.push_dedup(m);
        }
    }

    /// Current buffer depth for an assignee (0 if none). Observable for tests.
    pub fn buffered_for(&self, assignee: &str) -> usize {
        self.buffers
            .get(assignee)
            .map(|b| b.mentions.len())
            .unwrap_or(0)
    }

    /// Number of assignees with an open (non-empty) buffer.
    pub fn open_buffers(&self) -> usize {
        self.buffers
            .values()
            .filter(|b| !b.mentions.is_empty())
            .count()
    }

    /// Drain + coalesce one assignee's buffer.
    fn take_buffer(&mut self, assignee: &str) -> Option<Coalesced> {
        let buf = self.buffers.get_mut(assignee)?;
        if buf.mentions.is_empty() {
            return None;
        }
        let mentions: Vec<InboundMention> = buf.mentions.drain(..).collect();
        let first_arrival_ms = buf.first_arrival_ms;
        // Reset the buffer's window so the next mention opens a fresh one.
        buf.first_arrival_ms = 0;
        buf.last_arrival_ms = 0;
        Some(Coalesced {
            assignee: assignee.to_string(),
            mentions,
            first_arrival_ms,
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
        }
    }


    /// GS-062: requeue restores a flushed burst (acquire-drop / in-flight retry).
    #[test]
    fn requeue_restores_burst_for_retry() {
        let mut c = MentionCoalescer::new(
            Duration::from_millis(2_000),
            Duration::from_millis(500),
            64,
        );
        c.submit(m("oc", "e1", "hi"), 0);
        let batch = c.force_flush("oc").expect("flushed");
        assert_eq!(c.buffered_for("oc"), 0);
        c.requeue(batch, 1_000, true);
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
        let mut c = MentionCoalescer::new(
            Duration::from_millis(2_000),
            Duration::from_millis(500),
            64,
        );
        c.submit(
            InboundMention {
                assignee: "oc".into(),
                parent: "p1".into(),
                spec: serde_json::json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").expect("flushed");
        // InFlight path uses flush_ready=false (fresh window at now).
        c.requeue(batch, 1_000, false);
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

    /// GS-062 PARTIAL-KEEP-FULL-SPEC: retain_parents drops SkipAlreadyDelivered parents.
    #[test]
    fn retain_parents_filters_merged_spec() {
        let batch = Coalesced {
            assignee: "oc".into(),
            mentions: vec![
                InboundMention {
                    assignee: "oc".into(),
                    parent: "already".into(),
                    spec: serde_json::json!({"n": 1}),
                },
                InboundMention {
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
            // None should cap-flush (20 < 64); all buffer.
            assert!(
                matches!(dec, CoalesceDecision::Buffered { .. }),
                "mention {i} should buffer, got {dec:?}"
            );
        }
        // Force-flush at end of burst → exactly one coalesced batch of 20.
        let flushed = c.force_flush_all();
        assert_eq!(flushed.len(), 1, "one assignee → one batch");
        let batch = &flushed[0];
        assert_eq!(batch.assignee, "oc");
        assert_eq!(batch.count(), 20, "all 20 mentions coalesced");
        // Ordering preserved (mention 0 first, mention 19 last).
        assert_eq!(batch.mentions[0].spec["text"], "mention 0");
        assert_eq!(batch.mentions[19].spec["text"], "mention 19");
    }

    /// ADR-0004 invariant 5: a LONE mention dispatches at first-mention +
    /// sub-window, NOT at full window. (Common case pays no 2s latency.)
    #[test]
    fn lone_mention_flushes_at_subwindow_not_full_window() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        // Single mention arrives at t=0.
        c.submit(m("oc", "e1", "hi"), 0);
        // Before sub-window (t=400): not ready.
        assert_eq!(c.flush_if_ready("oc", 400), FlushOutcome::NotReady);
        // At sub-window (t=500): flushes (lone-mention fast path).
        let out = c.flush_if_ready("oc", 500);
        assert!(matches!(out, FlushOutcome::Flushed(_)), "sub-window flush");
        if let FlushOutcome::Flushed(batch) = out {
            assert_eq!(batch.count(), 1);
        }
    }

    /// A burst that KEEPS arriving stays buffered up to the full window (the
    /// sub-window fast path resets on each new mention).
    #[test]
    fn ongoing_burst_stays_buffered_until_full_window() {
        let mut c =
            MentionCoalescer::new(Duration::from_millis(2_000), Duration::from_millis(500), 64);
        // Mentions arrive every 300ms — faster than the 500ms sub-window, so
        // the burst is "ongoing" and must NOT flush early.
        for i in 0..5 {
            c.submit(m("oc", "e", &format!("m{i}")), i * 300);
        }
        // At t=1200 (after 4 sub-windows elapsed since last mention at 1200):
        // last_arrival=1200, sub-window not yet elapsed → still buffered.
        assert_eq!(c.flush_if_ready("oc", 1_200), FlushOutcome::NotReady);
        // At t=1700 (500ms after last arrival at 1200): sub-window flushes.
        let out = c.flush_if_ready("oc", 1_700);
        match out {
            FlushOutcome::Flushed(b) => assert_eq!(b.count(), 5),
            other => panic!("expected flush at t=1700, got {other:?}"),
        }
    }

    /// Hard cap flushes immediately (no infinite coalesce).
    #[test]
    fn hard_cap_flushes_now() {
        let mut c = MentionCoalescer::new(DEFAULT_WINDOW, DEFAULT_SUBWINDOW, 3);
        let _ = c.submit(m("oc", "e1", "a"), 0);
        let _ = c.submit(m("oc", "e2", "b"), 10);
        // Third mention hits the cap → FlushNow.
        let dec = c.submit(m("oc", "e3", "c"), 20);
        match dec {
            CoalesceDecision::FlushNow(batch) => {
                assert_eq!(batch.count(), 3);
                assert_eq!(batch.assignee, "oc");
            }
            other => panic!("expected FlushNow at cap, got {other:?}"),
        }
        // Buffer drained after the cap flush.
        assert_eq!(c.buffered_for("oc"), 0);
    }

    /// Dedup: identical (parent, spec) within one window collapses.
    #[test]
    fn identical_mentions_dedup() {
        let mut c = MentionCoalescer::with_defaults();
        let _ = c.submit(m("oc", "e1", "same"), 0);
        let dec = c.submit(m("oc", "e1", "same"), 10); // identical
        match dec {
            CoalesceDecision::Buffered { buffered } => assert_eq!(buffered, 1, "dup not added"),
            other => panic!("dup should buffer (not flush), got {other:?}"),
        }
        let batch = c.force_flush("oc").expect("flush");
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
        // Assignee-sorted: gk before oc.
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
        let first = c.force_flush("oc").expect("first flush");
        assert_eq!(first.count(), 1);
        // Well after the window: a new mention opens a fresh buffer.
        c.submit(m("oc", "e2", "second batch"), 5_000);
        let second = c.force_flush("oc").expect("second flush");
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
        // At t=500 both sub-window-flush.
        let flushed = c.tick(500);
        assert_eq!(flushed.len(), 2, "both assignees flush");
        // Assignee-sorted.
        assert_eq!(flushed[0].assignee, "gk");
        assert_eq!(flushed[1].assignee, "oc");
    }

    /// merged_spec produces the documented `{"coalesced":true,"count":N,...}`.
    #[test]
    fn merged_spec_shape() {
        let mut c = MentionCoalescer::with_defaults();
        c.submit(m("oc", "e1", "a"), 0);
        c.submit(m("oc", "e2", "b"), 10);
        let batch = c.force_flush("oc").expect("flush");
        let spec = batch.merged_spec();
        assert_eq!(spec["coalesced"], true);
        assert_eq!(spec["count"], 2);
        assert!(spec["mentions"].is_array());
        assert_eq!(spec["mentions"].as_array().unwrap().len(), 2);
    }

    /// Pure parse of the env value (no env mutation — `set_var` is unsafe in
    /// edition 2024; the env-override path is covered by `tests/coalesce_env.rs`).
    #[test]
    fn parse_millis_cases() {
        assert_eq!(
            parse_millis("123", DEFAULT_WINDOW),
            Duration::from_millis(123)
        );
        assert_eq!(
            parse_millis(" 123 ", DEFAULT_WINDOW),
            Duration::from_millis(123)
        );
        assert_eq!(
            parse_millis("0", DEFAULT_WINDOW),
            DEFAULT_WINDOW,
            "zero falls back"
        );
        assert_eq!(parse_millis("garbage", DEFAULT_WINDOW), DEFAULT_WINDOW);
        assert_eq!(parse_millis("", DEFAULT_WINDOW), DEFAULT_WINDOW);
    }

    /// open_buffers counts only non-empty buffers.
    #[test]
    fn open_buffers_count() {
        let mut c = MentionCoalescer::with_defaults();
        assert_eq!(c.open_buffers(), 0);
        c.submit(m("oc", "e1", "a"), 0);
        assert_eq!(c.open_buffers(), 1);
        c.submit(m("gk", "e2", "b"), 0);
        assert_eq!(c.open_buffers(), 2);
        let _ = c.force_flush("oc");
        assert_eq!(c.open_buffers(), 1);
    }
}
