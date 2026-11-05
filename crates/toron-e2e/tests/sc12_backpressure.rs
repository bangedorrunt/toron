// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-12 — Burst backpressure (A7 bounded dispatch pool) [P2].
//!
//! Actors: 20 @mentions to OC-β in 1s.
//! Assert:
//! - in-flight ≤ N (config semaphore)
//! - one-in-flight-per-agent honored
//! - no herdr swarm overload
//! - no lost jobs (queued, not dropped)
//! - ordering preserved
//!
//! Drives `toron_workflow::dispatch::DispatchPool` (the A7 mechanism — Buzz
//! claim/return pattern) with a low `max_concurrency` to force contention.
//!
//! Gate: `cargo test -p toron-e2e --features p2`.
#![cfg(feature = "p2")]

use toron_workflow::dispatch::{DispatchConfig, DispatchPool, QueuedJob};

fn job(i: usize) -> QueuedJob {
    QueuedJob {
        job_id: format!("job-{i}"),
        assignee: "OCBeta".into(),
        parent_id: "trigger-1".into(),
        spec: serde_json::json!({ "mention": format!("@OCBeta task {i}") }),
    }
}

#[test]
fn sc12_burst_backpressure_bounded_and_ordered() {
    let pool = DispatchPool::new(DispatchConfig {
        max_concurrency: 3,
        ..DispatchConfig::default()
    });

    for i in 0..20 {
        let depth = pool.enqueue(job(i));
        assert!(depth >= 1, "job {i} queued");
    }
    assert_eq!(pool.queued_len(), 20, "all 20 queued (none dropped)");

    let permit = pool.try_acquire("OCBeta").expect("first permit");
    assert!(
        pool.try_acquire("OCBeta").is_none(),
        "one-in-flight-per-agent honored"
    );
    assert_eq!(pool.in_flight_for("OCBeta"), 1, "in-flight == 1");

    let next = pool.next_ready();
    assert!(next.is_none(), "OCBeta already in-flight, no ready job");

    drop(permit);
    let ready = pool.next_ready().expect("next job after release");
    assert_eq!(ready.job_id, "job-0", "FIFO: first queued dispatched first");

    let p1 = pool.try_acquire("OCBeta").expect("p1");
    assert!(pool.try_acquire("OCBeta").is_none(), "still one-in-flight");
    drop(p1);
    let ready2 = pool.next_ready().expect("next after second release");
    assert_eq!(ready2.job_id, "job-1", "FIFO preserved: job-1 next");

    let pool_multi = DispatchPool::new(DispatchConfig {
        max_concurrency: 2,
        ..DispatchConfig::default()
    });
    for i in 0..5 {
        pool_multi.enqueue(job(i));
    }
    let a = pool_multi.try_acquire("AgentA").expect("A");
    let b = pool_multi.try_acquire("AgentB").expect("B");
    assert!(
        pool_multi.try_acquire("AgentC").is_none(),
        "global concurrency cap: in-flight ≤ N"
    );
    assert_eq!(pool_multi.in_flight_for("AgentA"), 1);
    assert_eq!(pool_multi.in_flight_for("AgentB"), 1);
    drop((a, b));
    assert_eq!(pool_multi.in_flight_for("AgentA"), 0, "release frees slot");
}

#[test]
fn sc12_ordering_preserved_across_full_burst() {
    let pool = DispatchPool::new(DispatchConfig {
        max_concurrency: 1,
        ..DispatchConfig::default()
    });
    for i in 0..20 {
        pool.enqueue(job(i));
    }
    assert_eq!(pool.queued_len(), 20);

    let mut dispatched = Vec::new();
    for _ in 0..20 {
        if let Some(j) = pool.next_ready() {
            dispatched.push(j.job_id);
        }
        if let Some(permit) = pool.try_acquire("OCBeta") {
            drop(permit);
        }
    }
    assert_eq!(
        dispatched.len(),
        20,
        "all 20 dispatched in FIFO order (got {})",
        dispatched.len()
    );
    let positions: Vec<usize> = dispatched
        .iter()
        .map(|id| id.split('-').nth(1).unwrap().parse::<usize>().unwrap())
        .collect();
    let mut sorted = positions.clone();
    sorted.sort_unstable();
    assert_eq!(positions, sorted, "dispatch order preserved (FIFO)");
}
