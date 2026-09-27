// governed-by: ADR-0005 (P4.2 — spawn await resume, Behavior Test Matrix row
// `spawn_await_resume`)
#![forbid(unsafe_code)]

//! SC-19 — `spawn` + `await: true` SUSPENDS and resumes on the job's terminal
//! event (Behavior Test Matrix row `spawn_await_resume`, ADR-0005 Spec 4a).
//!
//! A spawn-await is NOT an approval: the resolver is an AGENT that must
//! CLAIM + complete a job (kind-43001 emit → kind-43004 result). The run
//! parks on the job promise; when the result arrives the bus subscriber
//! resolves it (`resolve_spawn`) and the run RESUMES with the job's output
//! under `steps.<name>.result` — a FAILURE would need kind-43006 (SC-20),
//! not a decision.
//!
//! Run: `cargo test -p toron-e2e --features p5 --test sc19_spawn_await_resume`

use toron_core::testkit::ChildGuard;

use std::sync::Arc;
use std::time::Duration;

use toron_workflow::engine::{
    AsyncEffectRunner, ProfileResolver, RunInput, WorkflowEngine, run_workflow,
};
use toron_workflow::schema::parse_workflow;

fn noop_runner() -> AsyncEffectRunner {
    Arc::new(|_: &toron_workflow::engine::ActionInvocation| {
        Box::pin(async move { Ok(serde_json::Value::Null) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
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
async fn sc19_spawn_await_resumes_on_job_result() {
    let dir = std::env::temp_dir().join(format!("toron-sc19-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = free_port();
    let server_url = format!("http://127.0.0.1:{port}");
    let Some(mut server) = start_resonate_server(port, &dir) else {
        // GS-018: no `resonate` binary on this runner (herdr/resonate stack
        // absent) — environmental skip, not a product failure. Real proof
        // runs where the stack exists.
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    };
    wait_ready(&server_url).await;

    // 1. Engine runs the workflow; the `spawn` step parks on the job promise.
    //    The run is genuinely SUSPENDED (not done) when the result arrives.
    let engine = WorkflowEngine::remote_with_resolvers_and_async_effect(
        &server_url,
        housekeeper_resolver(),
        WorkflowEngine::default_workflow_resolver(),
        Some(noop_runner()),
    )
    .expect("engine");
    let wf = parse_workflow(
        r##"
name: spawn-resume
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
        run_id: Some("sc-test-run".into()),
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("sc19-spawn-resume", run_workflow, input.clone())
        .spawn()
        .await
        .expect("spawn run");

    // 2. Capture the parked promise id from the spawn sink (the embedding
    //    emits kind-43001 carrying it in `spec`; the promise↔job binding key).
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

    // The run must be genuinely SUSPENDED at this point — resolving a promise
    // for an already-completed run would make the test vacuous.
    assert!(
        !handle.done().await.expect("done"),
        "run must be SUSPENDED (parked on the spawn-await), not done, before the result"
    );

    // 3. The job agent completes: kind-43004 result arrives → the bus
    //    subscriber calls resolve_spawn (the 43004 side of the binding).
    engine
        .resolve_spawn(
            &promise_id,
            serde_json::json!({ "exit": 0, "checks": "all-healthy" }),
        )
        .await
        .expect("job result (kind-43004) resolves the spawn promise");

    // 4. The parked run resumes from its checkpoint and completes.
    let output = handle
        .result()
        .await
        .expect("run resumes after the job result and completes, NOT failed");

    assert_eq!(
        output["build"]["ok"], true,
        "spawn-await resumes to SUCCESS with the job result: {output}"
    );
    assert_eq!(
        output["build"]["result"]["exit"], 0,
        "job result payload flows into steps.build.result: {output}"
    );
    assert_eq!(
        output["build"]["result"]["checks"], "all-healthy",
        "job result payload flows into steps.build.result: {output}"
    );

    server.kill();
    let _ = std::fs::remove_dir_all(&dir);
}
