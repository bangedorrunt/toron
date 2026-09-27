// governed-by: ADR-0005 (P5.1 — Resonate concurrency + replay-determinism spike, GATE)
#![forbid(unsafe_code)]

//! SC-16 — Resonate concurrency + replay-determinism spike (P5 GATE).
//!
//! **What this proves (ADR-0005 Spec 4b):** concurrent durable children inside
//! ONE deterministic-replay function, both suspending on external promises,
//! resume in REVERSED order, and replay BIT-IDENTICALLY after a crash. This is
//! the primitive the DAG engine (`graph:`) is gated on — if it is not
//! deterministic, concurrent fan-out corrupts durable state.
//!
//! The root workflow spawns TWO children concurrently (`ctx.run().spawn()`).
//! Each child parks on its own external promise (`ctx.promise().await`) and
//! reports `(promise_id, node)` on a side-channel. The test:
//!   1. runs the root on Engine A → both children park (ids captured),
//!   2. CRASHES the worker (drops Engine A — the Resonate server retains the
//!      durable execution + both parked promises),
//!   3. RESOLVES the promises in REVERSED order on a fresh Engine B
//!      (child b's promise FIRST, then child a's),
//!   4. re-issues the same run id on Engine B → resumes from checkpoint →
//!      completes,
//!   5. asserts the replay is BIT-IDENTICAL:
//!      - same step outputs (a→{n:1}, b→{n:2}; reversed resolution must NOT
//!        swap them),
//!      - same promise-id assignment (post-crash ids == pre-crash ids),
//!      - same child-spawn order (child a's output stays under `a`).
//!
//! **Gate:** PASS → P5 unblocked. FAIL → P5 deferred; `parallel over:` stays
//! sequential (p3-auto-crew staging).
//!
//! Run: `cargo test -p toron-e2e --features p5 --test sc16_concurrency_spike`

use std::sync::Arc;
use std::time::Duration;
use toron_core::testkit::ChildGuard;

use resonate_sdk::prelude::{Resonate, ResonateConfig};
use resonate_sdk::types::Value;
use serde_json::json;

/// Side-channel for children to report `(promise_id, node)`. Per-engine, so
/// pre-crash (engine A) and post-replay (engine B) assignments compare.
pub type SpikeSink = Arc<tokio::sync::Mutex<Vec<(String, String)>>>;

/// The ROOT workflow: spawns two children CONCURRENTLY, awaits both. Each
/// child suspends on an external promise. This is the DAG-shaped topology the
/// spike must prove deterministic.
// `input` is unused by design (the root takes no arguments) but the
// resonate durable-function shape requires the (ctx, input) signature.
#[resonate_sdk::function]
async fn spike_root(
    ctx: &Context,
    input: serde_json::Value,
) -> resonate_sdk::Result<serde_json::Value> {
    let _ = input;
    // True concurrency via `.spawn()` — both children run in parallel.
    let f_a = ctx.run(spike_child, json!({ "node": "a" })).spawn()?;
    let f_b = ctx.run(spike_child, json!({ "node": "b" })).spawn()?;
    let a = f_a.await?;
    let b = f_b.await?;
    Ok(json!({ "a": a, "b": b }))
}

/// A CHILD workflow: parks on an external promise, reports `(promise_id,
/// node)` on the side-channel, then returns the resolved value.
#[resonate_sdk::function]
async fn spike_child(
    ctx: &Context,
    input: serde_json::Value,
) -> resonate_sdk::Result<serde_json::Value> {
    let node = input["node"].as_str().unwrap_or("?").to_string();
    let pending = ctx.promise::<serde_json::Value>().data(&input)?.create()?;
    let pid = pending.id().await?;
    ctx.get_dependency::<SpikeSink>()
        .lock()
        .await
        .push((pid.clone(), node.clone()));
    let resolved = pending.await?;
    Ok(json!({ "node": node, "promise_id": pid, "resolved": resolved }))
}

/// Pick a free localhost port for the Resonate server.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Start a persistent `resonate serve` (sqlite) child process. The child is
/// the "server" — killing the WORKER (engine) must not lose its state.
fn start_resonate_server(port: u16, dir: &std::path::Path) -> ChildGuard {
    toron_core::testkit::ChildGuard::resonate_serve(port, dir)
        .expect("spawn resonate serve")
        .expect("resonate binary present (GS-018: absent on hosted CI runners)")
}

async fn wait_ready(url: &str) {
    for _ in 0..100 {
        let url = url.to_string();
        if tokio::task::spawn_blocking(move || tcp_probe(&url))
            .await
            .ok()
            .flatten()
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("resonate server at {url} never became ready");
}

/// Minimal TCP liveness probe (no extra deps).
fn tcp_probe(url: &str) -> Option<bool> {
    std::net::TcpStream::connect_timeout(
        &url.replace("http://", "").parse().expect("socket addr"),
        Duration::from_millis(200),
    )
    .map(|_| true)
    .ok()
}

/// Build a fresh remote engine (worker) against the server URL.
fn engine(url: &str, sink: SpikeSink) -> anyhow::Result<Arc<Resonate>> {
    let config = ResonateConfig {
        url: Some(url.to_string()),
        ..Default::default()
    };
    let resonate = Resonate::new(config).with_dependency(sink);
    resonate
        .register(spike_root)
        .map_err(|e| anyhow::anyhow!("register spike_root: {e}"))?;
    resonate
        .register(spike_child)
        .map_err(|e| anyhow::anyhow!("register spike_child: {e}"))?;
    Ok(Arc::new(resonate))
}

/// Poll the side-channel until BOTH children have parked (2 entries).
async fn poll_parked(sink: &SpikeSink) -> Vec<(String, String)> {
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let entries = sink.lock().await.clone();
        if entries.len() >= 2 {
            return entries;
        }
    }
    panic!(
        "children never parked on their promises: got {:?}",
        sink.lock().await
    );
}

/// Resolve a promise by id with a value.
async fn resolve(resonate: &Resonate, id: &str, value: serde_json::Value) -> anyhow::Result<()> {
    let v = Value::from_serializable(value)
        .map_err(|e| anyhow::anyhow!("serialize resolve payload: {e}"))?;
    resonate
        .promises
        .resolve(id, v)
        .await
        .map_err(|e| anyhow::anyhow!("resolve {id}: {e}"))?;
    Ok(())
}

/// `resonate` presence probe (GS-018): the CI runner has no herdr/resonate
/// stack. Absence is environmental, not a product failure — skip loud and
/// let dev machines (stack adjacent) run the real proof.
fn has_resonate() -> bool {
    std::process::Command::new("resonate")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

#[tokio::test]
async fn concurrency_spike() {
    if !has_resonate() {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    }
    let dir = std::env::temp_dir().join(format!("toron-sc16-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");

    // 1. Persistent Resonate server (sqlite — durable server-side).
    let mut server = start_resonate_server(port, &dir);
    wait_ready(&server_url).await;

    let input = json!({});

    // 2. Engine A runs the root → two concurrent children park on their promises.
    let sink_a: SpikeSink = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let engine_a = engine(&server_url, sink_a.clone()).expect("engine A");
    let handle_a = engine_a
        .run("sc16-spike", spike_root, input.clone())
        .spawn()
        .await
        .expect("spawn run A");

    let parked = poll_parked(&sink_a).await;
    assert_eq!(
        parked.len(),
        2,
        "both children must park (2 external promises)"
    );

    // Map promise-id → node (each child reported its own).
    let pid_a = parked
        .iter()
        .find(|(_, n)| n == "a")
        .expect("pid a")
        .0
        .clone();
    let pid_b = parked
        .iter()
        .find(|(_, n)| n == "b")
        .expect("pid b")
        .0
        .clone();
    assert_ne!(pid_a, pid_b, "two distinct promise ids");

    // The run is genuinely PARKED (not completed) when we crash it — the whole
    // point of the spike is crash-while-suspended. A run that had already
    // completed would make the test vacuous.
    assert!(
        !handle_a.done().await.expect("done"),
        "run must be SUSPENDED (parked on both children), not done, before the crash"
    );

    // 3. CRASH THE WORKER: drop Engine A entirely. The server retains the
    //    durable execution + BOTH parked promises.
    drop(handle_a);
    drop(engine_a);

    // 4. Fresh worker (Engine B) on the same server. Resolve in REVERSED
    //    order: child b's promise FIRST, then child a's.
    let sink_b: SpikeSink = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let engine_b = engine(&server_url, sink_b.clone()).expect("engine B");
    resolve(&engine_b, &pid_b, json!({ "n": 2 }))
        .await
        .expect("resolve child b FIRST (reversed)");
    resolve(&engine_b, &pid_a, json!({ "n": 1 }))
        .await
        .expect("resolve child a SECOND");

    // 5. Re-issue the SAME run id on the fresh engine → resumes the existing
    //    execution from its checkpoint (both promises now resolved) → completes.
    let output = engine_b
        .run("sc16-spike", spike_root, input)
        .await
        .expect("run resumes and completes, NOT failed");

    // ── Bit-identical replay assertions ──────────────────────────────

    // (a) Same step outputs: child a's value stays under `a`, child b's under
    //     `b` — reversed resolution must NOT swap them.
    assert_eq!(
        output["a"]["node"], "a",
        "child a keeps its identity under reversed resume: {output}"
    );
    assert_eq!(
        output["b"]["node"], "b",
        "child b keeps its identity under reversed resume: {output}"
    );
    assert_eq!(
        output["a"]["resolved"],
        json!({ "n": 1 }),
        "child a received ITS resolved value (not child b's): {output}"
    );
    assert_eq!(
        output["b"]["resolved"],
        json!({ "n": 2 }),
        "child b received ITS resolved value (not child a's): {output}"
    );

    // (b) Same promise-id assignment: the ids the run used after the crash
    //     match the ids captured BEFORE the crash.
    let mut ids_a: Vec<String> = parked.iter().map(|(id, _)| id.clone()).collect();
    let mut ids_b: Vec<String> = sink_b
        .lock()
        .await
        .iter()
        .map(|(id, _)| id.clone())
        .collect();
    ids_a.sort();
    ids_b.sort();
    assert_eq!(
        ids_a,
        ids_b,
        "promise-id assignment is identical across crash/replay: pre={parked:?} post={:?}",
        sink_b.lock().await
    );

    // (c) The promise ids embedded in the output match the captured ones.
    let out_a = output["a"]["promise_id"].as_str().unwrap_or_default();
    let out_b = output["b"]["promise_id"].as_str().unwrap_or_default();
    assert!(
        parked.iter().any(|(id, n)| n == "a" && id == out_a)
            && parked.iter().any(|(id, n)| n == "b" && id == out_b),
        "output embeds the SAME promise ids the run parked on: {parked:?} vs {out_a}/{out_b}"
    );

    // Shut the server down.
    server.kill();
    let _ = std::fs::remove_dir_all(&dir);
}
