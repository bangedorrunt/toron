// governed-by: ADR-0005 (P4.2 — spawn await failure, Behavior Test Matrix row
// `spawn_await_failure`)
#![forbid(unsafe_code)]

//! SC-20 — `spawn` + `await: true` FAILS on the job's error event (Behavior
//! Test Matrix row `spawn_await_failure`, ADR-0005 Spec 4a).
//!
//! A spawn-await failure is NOT a decision: kind-43006 (job error) → the bus
//! subscriber rejects the promise (`reject_spawn`) → the step FAILS → Spec 0
//! propagates the failure (unlike `request_approval`, where rejection is a
//! DECISION and the run continues).
//!
//! Run: `cargo test -p toron-e2e --features p5 --test sc20_spawn_await_failure`

use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use toron_workflow::engine::{
    ActionRunner, ProfileResolver, RunInput, WorkflowEngine, run_workflow,
};
use toron_workflow::schema::{Action, parse_workflow};

fn noop_runner() -> ActionRunner {
    Arc::new(|_a: &Action| Ok(serde_json::Value::Null))
}

fn housekeeper_resolver() -> ProfileResolver {
    Arc::new(|name: &str| {
        if name == "@housekeeper" {
            Some(serde_json::json!({
                "npub": "npub1housekeeper",
                "spec": { "harness": "pi", "model": "claude-sonnet-4", "backend": "herdr" },
            }))
        } else {
            None
        }
    })
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn start_resonate_server(port: u16, dir: &std::path::Path) -> Child {
    Command::new("resonate")
        .args([
            "serve",
            "--storage-type",
            "sqlite",
            "--server-port",
            &port.to_string(),
            "--server-host",
            "127.0.0.1",
        ])
        .current_dir(dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn resonate server")
}

async fn wait_ready(url: &str) {
    for _ in 0..100 {
        let url = url.to_string();
        if tokio::task::spawn_blocking(move || {
            std::net::TcpStream::connect_timeout(
                &url.replace("http://", "").parse().expect("addr"),
                Duration::from_millis(200),
            )
            .is_ok()
        })
        .await
        .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("resonate server at {url} never became ready");
}

#[tokio::test]
async fn sc20_spawn_await_fails_on_job_error() {
    let dir = std::env::temp_dir().join(format!("toron-sc20-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let mut server = start_resonate_server(port, &dir);
    wait_ready(&server_url).await;

    // 1. Engine runs the workflow; the `spawn` step parks on the job promise.
    let engine = WorkflowEngine::remote_with(&server_url, noop_runner(), housekeeper_resolver())
        .expect("engine");
    let wf = parse_workflow(
        r##"
name: spawn-failure
trigger:
  type: webhook
steps:
  - name: build
    action: spawn
    profile: "@housekeeper"
    workflow: docker-upgrade
    await: true
    no_claim_timeout: 30
"##,
    )
    .expect("parse");
    let input = RunInput {
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("sc20-spawn-failure", run_workflow, input.clone())
        .spawn()
        .await
        .expect("spawn run");

    // 2. Capture the parked promise id from the spawn sink.
    let promise_id = {
        let mut got = None;
        for _ in 0..200 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let mut sink = engine.spawn_sink().lock().await;
            if let Some(id) = sink.pop() {
                got = Some(id);
                break;
            }
        }
        got.expect("run parked on a spawn-await promise (kind-43001 pending)")
    };

    assert!(
        !handle.done().await.expect("done"),
        "run must be SUSPENDED (parked on the spawn-await), not done, before the error"
    );

    // 3. The job agent FAILS: kind-43006 arrives → the bus subscriber calls
    //    reject_spawn (the 43006 side of the binding).
    engine
        .reject_spawn(&promise_id, "docker-upgrade failed: health-gate timeout")
        .await
        .expect("job error (kind-43006) rejects the spawn promise");

    // 4. The parked run resumes from its checkpoint: the step FAILED (not a
    //    decision) — Spec 0 propagates the failure. The step output carries
    //    ok:false with the job's error.
    let output = handle
        .result()
        .await
        .expect("run completes after the job error (step failed, not hung)");
    assert_eq!(
        output["build"]["ok"], false,
        "spawn-await failure fails the step, NOT a decision: {output}"
    );
    let err = output["build"]["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("health-gate timeout"),
        "the job's error surfaces in the step failure: {err}"
    );

    let _ = server.kill();
    let _ = server.wait();
    let _ = std::fs::remove_dir_all(&dir);
}
