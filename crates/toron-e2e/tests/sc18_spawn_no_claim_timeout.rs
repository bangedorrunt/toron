// governed-by: ADR-0005 (P4.2 — spawn no_claim_timeout abort, SC-18)
#![forbid(unsafe_code)]

//! SC-18 — `spawn` `no_claim_timeout` abort (Behavior Test Matrix row
//! `spawn_no_claim_timeout`, ADR-0005 Spec 4a).
//!
//! A spawn-await that is never claimed must ABORT after `no_claim_timeout` —
//! the run FAILS (a failure, not a decision). The timeout is a SERVER-side
//! timer (the promise's `timeout_at`), so this needs a real `resonate serve`
//! (unlike `Resonate::local`, which has no timer loop).
//!
//! Run: `cargo test -p toron-e2e --features p5 --test sc18_spawn_no_claim_timeout`

use toron_core::testkit::ChildGuard;

use std::sync::Arc;
use std::time::Duration;

use toron_workflow::engine::{AsyncEffectRunner, ProfileResolver, RunInput, WorkflowEngine};
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
async fn sc18_spawn_no_claim_timeout_aborts() {
    let dir = std::env::temp_dir().join(format!("toron-sc18-{}", uuid::Uuid::new_v4()));
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

    let engine = WorkflowEngine::remote_with_resolvers_and_async_effect(
        &server_url,
        housekeeper_resolver(),
        WorkflowEngine::default_workflow_resolver(),
        Some(noop_runner()),
    )
    .expect("engine");
    let wf = parse_workflow(
        r##"
name: spawn-timeout
trigger:
  type: webhook
steps:
  - name: build
    action: spawn
    profile: "@housekeeper"
    await: true
    no_claim_timeout: 2
"##,
    )
    .expect("parse");
    let input = RunInput {
        run_id: Some("sc-test-run".into()),
        workflow: wf,
        trigger: serde_json::json!({}),
    };

    // Never claim the job: the server-side timer should abort the await after
    // ~2s and the run completes with the step FAILED (not hung).
    let output = engine
        .run("sc18-spawn-timeout", input)
        .await
        .expect("run completes after claim timeout (not a hang)");
    assert_eq!(
        output["build"]["ok"], false,
        "unclaimed spawn-await aborts and fails the step: {output}"
    );
    let err = output["build"]["error"].as_str().unwrap_or_default();
    assert!(
        err.contains("timeout") || err.contains("rejected"),
        "timeout surfaced: {err}"
    );

    server.kill();
    let _ = std::fs::remove_dir_all(&dir);
}
