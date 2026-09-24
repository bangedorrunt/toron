// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Dispatch pool (bd-yt2, A7): bounded concurrency + one-in-flight-per-agent.
//!
//! Buzz `buzz-acp` claim/return pattern: @mention bursts must not thrash the
//! agent swarm. The pool enforces:
//! - a **bounded semaphore** (configurable global concurrency);
//! - **one-in-flight-per-agent** (a second job for an already-working agent
//!   QUEUES, never drops);
//! - queued jobs are **durable**: they live in the bus event store (the
//!   lossless-projection rule — never a dropping channel); the in-process
//!   queue is a single-writer cache over those rows and a restart rebuilds
//!   it via [`DispatchPool::rebuild_from_jobs`], resuming the cursor.
//!
//! This is the in-process dispatcher; the Resonate-backed durable runner
//! (bd-oge) layers crash-survivable execution on top.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use toron_core::busy_owner::{
    BusyOwnerEffect, BusyOwnerModel, BusyOwnerMsg, DEFAULT_HOLD_BOUND_MS, tick as busy_tick,
};

/// A job waiting to be dispatched (queued in-process, mirroring the durable
/// kind-43001 in the bus store).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueuedJob {
    /// The kind-43001 event id (the job's identity + bus cursor key).
    pub job_id: String,
    /// Target agent name.
    pub assignee: String,
    /// Parent (trigger) event id.
    pub parent_id: String,
    /// Job spec.
    #[serde(default)]
    pub spec: serde_json::Value,
}

/// Dispatch pool configuration.
#[derive(Debug, Clone, Copy)]
pub struct DispatchConfig {
    /// Global max in-flight jobs (bounded semaphore).
    pub max_concurrency: usize,
    /// Busy-owner hold bound (ms) — Buzz #7337/#7340; 0 disables timed hold.
    pub hold_bound_ms: u64,
}

impl Default for DispatchConfig {
    fn default() -> Self {
        // Buzz default: a pool of 100.
        Self {
            max_concurrency: 100,
            hold_bound_ms: DEFAULT_HOLD_BOUND_MS,
        }
    }
}

/// The in-process dispatch pool (A7).
pub struct DispatchPool {
    config: DispatchConfig,
    semaphore: Arc<Semaphore>,
    /// Per-agent in-flight count.
    in_flight: Arc<Mutex<HashMap<String, usize>>>,
    /// Queued jobs (in-process mirror; the bus store is the durable record).
    queue: Arc<Mutex<VecDeque<QueuedJob>>>,
    /// Busy-owner fence (generation + hold bound + deadline wake) — pure core.
    busy: Arc<Mutex<BusyOwnerModel>>,
}

impl DispatchPool {
    /// New pool.
    pub fn new(config: DispatchConfig) -> Self {
        let hold = config.hold_bound_ms;
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrency)),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            busy: Arc::new(Mutex::new(BusyOwnerModel::new(hold))),
            config,
        }
    }

    /// Enqueue a job (durable mirror; the caller persists the kind-43001 to
    /// the bus BEFORE calling this). Returns the queue depth.
    pub fn enqueue(&self, job: QueuedJob) -> usize {
        let mut q = self.queue.lock();
        q.push_back(job);
        q.len()
    }

    /// Rebuild the in-process queue from the durable record (bd-e03q,
    /// ADR-0035 Spec 4): the bus kind-43001s are the source of truth and
    /// this queue is their single-writer in-memory projection. Fresh
    /// processes replay the unacked rows in cursor order; the embedding
    /// sources them from the bus (the same filter `rebuild_store`-style
    /// readers use). Replaces whatever the cache held — a restart never
    /// merges stale memory with the truth.
    pub fn rebuild_from_jobs(config: DispatchConfig, jobs: Vec<QueuedJob>) -> Self {
        let pool = Self::new(config);
        {
            let mut q = pool.queue.lock();
            q.extend(jobs);
        }
        pool
    }

    /// Try to acquire a dispatch slot for `agent`. Returns a permit guard when
    /// the agent has no in-flight job AND global concurrency is available.
    /// Otherwise returns `None` (the job stays queued).
    pub fn try_acquire(&self, agent: &str) -> Option<DispatchPermit<'_>> {
        let mut in_flight = self.in_flight.lock();
        let count = in_flight.entry(agent.to_string()).or_insert(0);
        if *count >= 1 {
            return None; // one-in-flight-per-agent
        }
        // Check global concurrency without blocking.
        self.semaphore.clone().try_acquire_owned().map_or_else(
            |_| None, // global cap reached
            |permit| {
                *count += 1;
                Some(DispatchPermit {
                    pool: self,
                    agent: agent.to_string(),
                    _permit: permit,
                })
            },
        )
    }

    /// Pop the next queued job for an agent with a free slot, if any.
    /// Non-blocking — the caller re-checks when a permit is released.
    pub fn next_ready(&self) -> Option<QueuedJob> {
        let mut q = self.queue.lock();
        let idx = q.iter().position(|job| self.has_slot(&job.assignee))?;
        q.remove(idx)
    }

    fn has_slot(&self, agent: &str) -> bool {
        self.in_flight.lock().get(agent).copied().unwrap_or(0) == 0
            && self.semaphore.available_permits() > 0
    }

    /// Number of queued jobs.
    pub fn queued_len(&self) -> usize {
        self.queue.lock().len()
    }

    /// In-flight count for an agent (observable for tests).
    pub fn in_flight_for(&self, agent: &str) -> usize {
        self.in_flight.lock().get(agent).copied().unwrap_or(0)
    }

    /// Global concurrency limit.
    pub const fn max_concurrency(&self) -> usize {
        self.config.max_concurrency
    }

    /// Current busy-owner generation for an agent (0 if never forked).
    pub fn generation_of(&self, agent: &str) -> u64 {
        self.busy.lock().generation_of(agent)
    }

    /// Fork/rebind: advance generation so a stale worker cannot reclaim (Buzz #7340).
    pub fn fork_rebind(&self, agent: &str) -> Vec<BusyOwnerEffect> {
        let mut busy = self.busy.lock();
        let (next, fx) = busy_tick(&busy, &BusyOwnerMsg::ForkRebind {
            agent: agent.to_string(),
        });
        *busy = next;
        fx
    }

    /// Try acquire with busy-owner bound + generation fence.
    ///
    /// When the agent is busy, returns `Err(effects)` containing
    /// `SleepUntilDeadline` / `HoldContinued` / `HoldExpired` / `FencedStale`
    /// — the host journals Sleep (Resonate park); settlement ≠ provider done.
    pub fn try_acquire_fenced(
        &self,
        agent: &str,
        now_ms: u64,
        claimed_generation: u64,
    ) -> Result<DispatchPermit<'_>, Vec<BusyOwnerEffect>> {
        let mut busy = self.busy.lock();
        let (next, fx) = busy_tick(
            &busy,
            &BusyOwnerMsg::TryAcquire {
                agent: agent.to_string(),
                now_ms,
                claimed_generation,
            },
        );
        *busy = next;
        match fx.as_slice() {
            [BusyOwnerEffect::Acquired { .. }] => {
                drop(busy);
                self.try_acquire(agent).ok_or_else(|| {
                    // Global cap lost the race — surface as empty effects; caller requeues.
                    vec![]
                })
            }
            _ => Err(fx),
        }
    }

    /// Deadline wake for a held busy-owner wait (host supplies clock).
    pub fn deadline_wake(&self, agent: &str, now_ms: u64) -> Vec<BusyOwnerEffect> {
        let mut busy = self.busy.lock();
        let (next, fx) = busy_tick(
            &busy,
            &BusyOwnerMsg::DeadlineWake {
                agent: agent.to_string(),
                now_ms,
            },
        );
        *busy = next;
        fx
    }

}

impl std::fmt::Debug for DispatchPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DispatchPool")
            .field("max_concurrency", &self.config.max_concurrency)
            .field("queued", &self.queued_len())
            .finish()
    }
}

/// A held dispatch permit: releasing it frees the agent's in-flight slot.
pub struct DispatchPermit<'a> {
    pool: &'a DispatchPool,
    agent: String,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl Drop for DispatchPermit<'_> {
    fn drop(&mut self) {
        let mut in_flight = self.pool.in_flight.lock();
        if let Some(count) = in_flight.get_mut(&self.agent) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                in_flight.remove(&self.agent);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// bd-e03q RED (ADR-0035 Spec 4): the in-process queue rebuilds
    /// from the durable record — a fresh pool over the same job rows
    /// resumes the dispatch cursor in order.
    #[test]
    fn rebuild_from_jobs_restores_queue_after_restart() {
        let jobs = vec![
            QueuedJob {
                job_id: "j1".into(),
                assignee: "a".into(),
                parent_id: "p".into(),
                spec: serde_json::json!({}),
            },
            QueuedJob {
                job_id: "j2".into(),
                assignee: "b".into(),
                parent_id: "p".into(),
                spec: serde_json::json!({}),
            },
        ];
        // Fresh process, wiped cache: rebuild from the durable rows.
        let pool = DispatchPool::rebuild_from_jobs(DispatchConfig::default(), jobs);
        assert_eq!(pool.queued_len(), 2, "both rows restored");
        assert_eq!(pool.next_ready().expect("j1").job_id, "j1");
        assert_eq!(pool.next_ready().expect("j2").job_id, "j2");
        assert_eq!(pool.queued_len(), 0, "cursor drained");
    }

    #[test]
    fn one_in_flight_per_agent() {
        let pool = DispatchPool::new(DispatchConfig::default());
        let a = pool.try_acquire("agent-a").expect("first permit");
        // Second acquire for the same agent → None (one-in-flight).
        assert!(pool.try_acquire("agent-a").is_none());
        assert_eq!(pool.in_flight_for("agent-a"), 1);
        drop(a);
        // After release, the slot is free again.
        assert!(pool.try_acquire("agent-a").is_some());
    }

    #[test]
    fn global_concurrency_bounds() {
        let pool = DispatchPool::new(DispatchConfig { max_concurrency: 2, ..DispatchConfig::default() });
        let p1 = pool.try_acquire("a").expect("a");
        let p2 = pool.try_acquire("b").expect("b");
        // Global cap reached → third agent waits.
        assert!(pool.try_acquire("c").is_none());
        drop(p1);
        assert!(pool.try_acquire("c").is_some(), "slot freed after release");
        let _ = p2;
    }

    #[test]
    fn queued_jobs_persist_in_mirror() {
        let pool = DispatchPool::new(DispatchConfig::default());
        let q1 = QueuedJob {
            job_id: "j1".into(),
            assignee: "a".into(),
            parent_id: "p".into(),
            spec: serde_json::json!({}),
        };
        pool.enqueue(q1.clone());
        pool.enqueue(q1);
        assert_eq!(pool.queued_len(), 2);
        // Drains in FIFO order when slots are free.
        let popped = pool.next_ready().expect("first");
        assert_eq!(popped.job_id, "j1");
        assert_eq!(pool.queued_len(), 1);
    }

    #[test]
    fn fork_rebind_fences_stale_acquire() {
        let pool = DispatchPool::new(DispatchConfig {
            max_concurrency: 2,
            hold_bound_ms: 1_000,
        });
        let permit = pool.try_acquire("a").expect("acquire");
        let fx = pool.fork_rebind("a");
        assert!(matches!(
            fx[0],
            BusyOwnerEffect::GenerationAdvanced { generation: 1, .. }
        ));
        // Stale gen-0 claim is fenced (no second permit).
        let err = match pool.try_acquire_fenced("a", 50, 0) {
            Err(e) => e,
            Ok(_) => panic!("stale gen must be fenced"),
        };
        assert!(matches!(
            err[0],
            BusyOwnerEffect::FencedStale {
                claimed: 0,
                current: 1,
                ..
            }
        ));
        drop(permit);
    }

    #[test]
    fn busy_agent_job_stays_queued() {
        let pool = DispatchPool::new(DispatchConfig::default());
        let _permit = pool.try_acquire("a").expect("a busy");
        pool.enqueue(QueuedJob {
            job_id: "j1".into(),
            assignee: "a".into(),
            parent_id: "p".into(),
            spec: serde_json::json!({}),
        });
        // Agent a is busy → the job stays queued, not dispatched.
        assert!(pool.next_ready().is_none());
        assert_eq!(pool.queued_len(), 1);
    }
}
