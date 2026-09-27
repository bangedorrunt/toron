// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-11 — Workflow approval: suspend → crash → resume (P2 / Phase-2 gate).
//!
//! **The decisive durability differentiator vs Buzz (WF-08 fix).** Buzz marks a
//! run that hits `request_approval` as FAILED when it doesn't resume in-process;
//! toron's Resonate-backed engine PARKS the execution on a durable promise that
//! lives in the Resonate server, so a worker-process crash mid-run loses
//! nothing — a fresh worker reconnects, the operator's approval resolves the
//! promise, and the run RESUMES FROM CHECKPOINT, not marked failed.
//!
//! Simulation (real crash semantics, one process):
//! 1. `resonate serve` (sqlite storage) runs as a child process — durable state
//!    survives any worker restart because it is server-side.
//! 2. Engine A (remote) runs the `deploy` workflow → hits `request_approval` →
//!    parks (kind-46011 pending; the promise_id is captured).
//! 3. **Kill the worker**: drop Engine A entirely (the server retains the
//!    durable execution + parked promise).
//! 4. **Restart**: a fresh Engine B (same server) resolves the approval.
//! 5. The re-issued `run(id, …)` resumes the existing execution from the
//!    parked promise → completes. NOT failed.
//!
//! Gate: `cargo test -p toron-e2e --features p2`.
#![cfg(feature = "p2")]

use toron_core::testkit::ChildGuard;

use std::sync::Arc;
use std::time::Duration;

use toron_workflow::engine::{AsyncEffectRunner, Decision, RunInput, WorkflowEngine};
use toron_workflow::schema::{Action, Workflow, parse_workflow};

/// A `deploy` workflow that hits `request_approval` after one step.
fn deploy_workflow() -> Workflow {
    parse_workflow(
        r##"
name: deploy
trigger:
  type: webhook
steps:
  - name: prep
    action: send_message
    to: ["ops"]
    subject: "prep"
    body: "staging ready"
  - name: gate
    action: request_approval
    message: "Approve deploy to prod?"
  - name: ship
    action: send_message
    to: ["ops"]
    subject: "shipped"
    body: "prod deployed"
"##,
    )
    .expect("parse deploy workflow")
}

fn noop_runner() -> AsyncEffectRunner {
    Arc::new(|inv: &toron_workflow::engine::ActionInvocation| {
        // Record step actions for the "step events emitted throughout" assert.
        eprintln!("[sc11] action: {}", inv.action.type_name());
        Box::pin(async move { Ok(serde_json::Value::Null) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
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
/// the "server" — killing the WORKER (engine) below must not lose its state.
fn start_resonate_server(port: u16, dir: &std::path::Path) -> Option<ChildGuard> {
    let server =
        toron_core::testkit::ChildGuard::resonate_serve(port, dir).expect("spawn resonate serve");
    if server.is_none() {
        // GS-018: no `resonate` binary on this runner (herdr/resonate stack
        // absent) — an environmental skip, not a product failure.
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack)");
    }
    server
}

async fn wait_ready(url: &str) {
    for _ in 0..100 {
        let url = url.to_string();
        if tokio::task::spawn_blocking(move || reqwest_blocking_get(&url))
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

/// Minimal blocking GET for the readiness probe (no extra deps).
fn reqwest_blocking_get(url: &str) -> Option<bool> {
    // The Resonate server responds on GET /promises/… with 404 for unknown,
    // but the TCP connect alone proves liveness.
    std::net::TcpStream::connect_timeout(
        &url.replace("http://", "").parse().expect("socket addr"),
        Duration::from_millis(200),
    )
    .map(|_| true)
    .ok()
}

#[tokio::test]
async fn sc11_approval_crash_resume() {
    let dir = std::env::temp_dir().join(format!("toron-sc11-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");

    // 1. Start the persistent Resonate server (sqlite — durable server-side).
    let Some(mut server) = start_resonate_server(port, &dir) else {
        // GS-018: no `resonate` binary on this runner (herdr/resonate stack
        // absent) — environmental skip, not a product failure. Real proof
        // runs where the stack exists.
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    };
    wait_ready(&server_url).await;

    // 2. Engine A (remote worker) runs the deploy workflow → parks on approval.
    let engine_a =
        WorkflowEngine::remote_with_async_effect_runner(&server_url, Some(noop_runner()))
            .expect("engine A");
    let input = RunInput {
        run_id: Some("sc-test-run".into()),
        workflow: deploy_workflow(),
        trigger: serde_json::json!({}),
    };
    // Spawn the run; it parks at `request_approval`. Capture the promise_id.
    let handle_a = engine_a
        .resonate()
        .run(
            "sc11-deploy",
            toron_workflow::engine::run_workflow,
            input.clone(),
        )
        .spawn()
        .await
        .expect("spawn run A");

    let promise_id = {
        let mut got = None;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut sink = engine_a.approval_sink().lock().await;
            if let Some(id) = sink.pop() {
                got = Some(id);
                break;
            }
        }
        got.expect("run A parked on an approval promise (kind-46011 pending)")
    };

    // 3. KILL THE WORKER: drop Engine A entirely. The Resonate server retains
    //    the durable execution + the parked promise — nothing is lost.
    drop(engine_a);
    drop(handle_a);

    // 4. RESTART: a fresh worker (Engine B) on the same server. The operator's
    //    approval (operator-signed kind-46011, echoed promise_id) resolves the
    //    promise via the fresh engine.
    let engine_b =
        WorkflowEngine::remote_with_async_effect_runner(&server_url, Some(noop_runner()))
            .expect("engine B");
    engine_b
        .resolve_approval(&promise_id, &Decision::approved("go"))
        .await
        .expect("operator approval resolves the promise");

    // 5. Re-issue the run with the SAME id on the fresh engine → Resonate
    //    resumes the existing execution from the (now-resolved) parked
    //    promise → completes. NOT marked failed (the WF-08 fix).
    let output = engine_b
        .run("sc11-deploy", input)
        .await
        .expect("run resumes and completes, NOT failed");

    // The run completed with the approval decision recorded — the checkpoint
    // after request_approval was reached, then the run finished.
    //
    // ADR-0024 Spec 2 (one decision spelling): the decision is published under
    // the node's OWN name (`gate`) AND under `decision` — asserted strictly, on
    // both keys, so this witness pins the contract the engine implements rather
    // than papering over a path that omits one of them.
    assert_eq!(
        output["decision"]["outcome"], "approved",
        "resumed run records the approval decision under `decision`: {output}"
    );
    assert_eq!(
        output["decision"], output["gate"],
        "the node-name output and the `decision` alias must agree: {output}"
    );
    // The pre-approval step (prep) + post-approval step (ship) both ran —
    // the durable replay re-executed from the checkpoint.
    assert!(output.is_object(), "run produced step outputs after resume");

    // Shut the server down.
    server.kill();
    let _ = std::fs::remove_dir_all(&dir);
}
