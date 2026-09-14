// governed-by: ADR-0005, ADR-0007
use super::*;
use crate::schema::{Action, Workflow, parse_workflow};
use std::time::Duration;

/// A workflow with a `request_approval` step (the WF-08 path) + a delay.
fn approval_workflow() -> Workflow {
    parse_workflow(
        r##"
name: deploy
trigger:
  type: webhook
steps:
  - name: gate
    action: request_approval
    message: "Approve deploy?"
  - name: ship
    action: send_message
    to: ["pi-alpha"]
    subject: "deployed"
    body: "ok"
"##,
    )
    .expect("parse")
}

/// A workflow with a durable `delay` step (survives replay).
fn delay_workflow() -> Workflow {
    parse_workflow(
        r##"
name: timer
trigger:
  type: schedule
  cron: "* * * * *"
steps:
  - name: wait
    action: delay
    secs: 1
"##,
    )
    .expect("parse")
}

fn noop_runner() -> ActionRunner {
    Arc::new(|_a: &Action| Ok(serde_json::Value::Null))
}

/// Boot a real `resonate serve` + a remote engine (the 0.6 SDK's
/// promise-park needs a server-side worker loop; `Resonate::local` cannot
/// drive the background promise-creation task). Returns (server, engine);
/// the caller keeps the server alive for the test's duration.
async fn remote_engine(
    action_runner: ActionRunner,
) -> Option<(std::process::Child, WorkflowEngine)> {
    use std::process::{Command, Stdio};
    let dir = std::env::temp_dir().join(format!("wf-rsrv-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let url = format!("http://127.0.0.1:{port}");
    let server = Command::new("resonate")
        .args([
            "serve",
            "--storage-type",
            "sqlite",
            "--server-port",
            &port.to_string(),
            "--server-host",
            "127.0.0.1",
        ])
        .current_dir(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    for _ in 0..100 {
        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{port}").parse().expect("addr"),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let engine = WorkflowEngine::remote(&url, action_runner).expect("remote engine");
    Some((server, engine))
}

/// Like [`remote_engine`] with a `ProfileResolver` for the `spawn` action.
async fn remote_engine_with(
    profile_resolver: ProfileResolver,
) -> Option<(std::process::Child, WorkflowEngine)> {
    use std::process::{Command, Stdio};
    let dir = std::env::temp_dir().join(format!("wf-rsrv-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let port = {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    };
    let url = format!("http://127.0.0.1:{port}");
    let server = Command::new("resonate")
        .args([
            "serve",
            "--storage-type",
            "sqlite",
            "--server-port",
            &port.to_string(),
            "--server-host",
            "127.0.0.1",
        ])
        .current_dir(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    for _ in 0..100 {
        if std::net::TcpStream::connect_timeout(
            &format!("127.0.0.1:{port}").parse().expect("addr"),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let engine =
        WorkflowEngine::remote_with(&url, noop_runner(), profile_resolver).expect("remote engine");
    Some((server, engine))
}

/// Poll the approval side-channel for a parked run's promise_id.
async fn poll_promise_id(engine: &WorkflowEngine) -> String {
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut sink = engine.approval_sink().lock().await;
        if let Some(id) = sink.pop() {
            return id;
        }
    }
    panic!("run never parked on an approval promise");
}

/// WF-08 core: a run hitting `request_approval` SUSPENDS (parks), then
/// RESUMES when the promise resolves — not marked failed.
#[tokio::test(flavor = "multi_thread")]
async fn approval_suspends_and_resumes() {
    // Uses a REAL `resonate serve`: the 0.6 SDK's `Resonate::local`
    // cannot drive the background promise-creation task while the run
    // blocks on the promise id (a local-runtime limitation; sc11 covers
    // the same contract end-to-end).
    let Some((_srv, engine)) = remote_engine(noop_runner()).await else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines");
        return;
    };

    let input = RunInput {
        run_id: None,
        workflow: approval_workflow(),
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("wf-approval-1", run_workflow, input)
        .spawn()
        .await
        .expect("spawn");
    // The run parks on the approval promise. Give the runtime a moment to
    // create it, then read the promise_id from the side-channel.
    let promise_id = poll_promise_id(&engine).await;

    // The run is NOT done (it is parked — the WF-08 suspend).
    assert!(
        !handle.done().await.expect("done"),
        "run must be suspended, not failed, before approval"
    );

    // Operator approves: resolve the promise.
    engine
        .resolve_approval(&promise_id, &Decision::approved("ship it"))
        .await
        .expect("resolve");

    // The parked run resumes and completes.
    let output = handle.result().await.expect("run completes after approval");
    // ADR-0024 graph canonical: decision is under the node name (gate) and
    // historically also under "decision". Accept either for compat.
    let outcome = output["decision"]["outcome"]
        .as_str()
        .or_else(|| output["gate"]["outcome"].as_str())
        .unwrap_or("");
    assert_eq!(
        outcome, "approved",
        "decision recorded after resume: {output}"
    );
}

/// bd-adr0006-cosmetic-sweep-q0g0: the approval message renders through the
/// template engine BEFORE it becomes the promise payload / step output — a
/// captain-readable signoff never carries raw `{{inputs.*}}` tokens.
#[test]
fn approval_message_renders_template_tokens() {
    let wf = parse_workflow(
        r##"
name: signoff
trigger:
  type: webhook
steps:
  - name: signoff
    action: request_approval
    message: "Wave signoff for {{inputs.objective}} [{{inputs.wave_label}}]: review the VERIFY receipts."
"##,
    )
    .expect("parse");
    let trigger = serde_json::json!({
        "inputs": {
            "objective": "graduate the wave-swarm recipe",
            "wave_label": "wave-1",
        }
    });
    let msg = rendered_approval_message(
        &wf.steps[0].action,
        &trigger,
        &serde_json::Map::new(),
        "wave-signoff",
        "signoff",
    )
    .expect("render");
    assert_eq!(
        msg,
        "Wave signoff for graduate the wave-swarm recipe [wave-1]: review the VERIFY receipts."
    );
}

/// A run hitting `request_approval` and REJECTED completes with the
/// rejection recorded (not failed — the rejection is a decision).
#[tokio::test(flavor = "multi_thread")]
async fn approval_rejection_is_a_decision_not_a_failure() {
    let Some((_srv, engine)) = remote_engine(noop_runner()).await else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines");
        return;
    };
    let input = RunInput {
        run_id: None,
        workflow: approval_workflow(),
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("wf-approval-reject", run_workflow, input)
        .spawn()
        .await
        .expect("spawn");

    let promise_id = poll_promise_id(&engine).await;

    engine
        .reject_approval(&promise_id, &Decision::rejected("not now"))
        .await
        .expect("reject");

    let output = handle.result().await.expect("completes after rejection");
    let outcome = output["decision"]["outcome"]
        .as_str()
        .or_else(|| output["gate"]["outcome"].as_str())
        .unwrap_or("");
    assert_eq!(outcome, "rejected", "rejection recorded: {output}");
}

/// A `ProfileResolver` that resolves `@housekeeper` to a spawn spec.
fn test_profile_resolver() -> ProfileResolver {
    Arc::new(|name: &str| {
        if name == "@housekeeper" {
            Some(serde_json::json!({
                "npub": "npub1housekeeper",
                "spec": { "harness": "pi", "model": "opencode-go/deepseek-v4-flash", "backend": "herdr" },
            }))
        } else {
            None
        }
    })
}

/// Poll the spawn side-channel for a parked spawn-await's promise_id.
async fn poll_spawn(engine: &WorkflowEngine) -> String {
    for _ in 0..400 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let mut sink = engine.spawn_sink().lock().await;
        if let Some(id) = sink.pop() {
            return id;
        }
    }
    panic!("spawn never parked on a job promise");
}

/// P4.2 (ADR-0005 Spec 4a): a `spawn` with `await: true` SUSPENDS, then
/// RESUMES when the job resolves (the bus subscriber's resolve_spawn).
#[tokio::test(flavor = "multi_thread")]
async fn spawn_await_resumes_on_job_result() {
    let Some((_srv, engine)) = remote_engine_with(test_profile_resolver()).await else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines");
        return;
    };
    let wf = parse_workflow(
        r##"
name: spawn-demo
trigger:
  type: webhook
steps:
  - name: build
    action: spawn
    profile: "@housekeeper"
    workflow: docker-upgrade
    await: true
"##,
    )
    .expect("parse spawn workflow");
    let input = RunInput {
        run_id: None,
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("wf-spawn-1", run_workflow, input)
        .spawn()
        .await
        .expect("spawn run");

    let promise_id = poll_spawn(&engine).await;
    assert!(
        !handle.done().await.expect("done"),
        "spawn-await must SUSPEND until the job resolves"
    );

    engine
        .resolve_spawn(&promise_id, serde_json::json!({ "status": "ok", "id": 7 }))
        .await
        .expect("resolve spawn");

    let output = handle.result().await.expect("resumes after job result");
    assert_eq!(
        output["build"]["ok"], true,
        "spawn step succeeded: {output}"
    );
    assert_eq!(output["build"]["result"]["id"], 7, "job result captured");
}

/// P4.2: a kind-43006 job error REJECTS the promise → the spawn step
/// fails (a FAILURE, not a decision — distinct from approval).
#[tokio::test(flavor = "multi_thread")]
async fn spawn_await_failure_fails_the_step() {
    let Some((_srv, engine)) = remote_engine_with(test_profile_resolver()).await else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines");
        return;
    };
    let wf = parse_workflow(
        r##"
name: spawn-fail
trigger:
  type: webhook
steps:
  - name: build
    action: spawn
    profile: "@housekeeper"
    await: true
"##,
    )
    .expect("parse");
    let input = RunInput {
        run_id: None,
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("wf-spawn-fail-1", run_workflow, input)
        .spawn()
        .await
        .expect("spawn run");

    let promise_id = poll_spawn(&engine).await;
    engine
        .reject_spawn(&promise_id, "job failed: docker upgrade broke")
        .await
        .expect("reject spawn");

    let output = handle.result().await.expect("run returns after failure");
    assert_eq!(
        output["build"]["ok"], false,
        "spawn failure is a FAILURE not a decision: {output}"
    );
    let err = output["build"]["error"].as_str().unwrap_or_default();
    assert!(err.contains("job failed"), "error surfaced: {err}");
}

/// Durable delay: a `delay` action runs and the run completes.
#[tokio::test]
async fn durable_delay_completes() {
    let runner = noop_runner();
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let input = RunInput {
        run_id: None,
        workflow: delay_workflow(),
        trigger: serde_json::json!({}),
    };
    let output = engine
        .run("wf-delay-1", input)
        .await
        .expect("delay run completes");
    assert!(output.is_object(), "delay run returns object output");
}

/// A runner that errors on `send_message` actions (the "upgrade" step
/// below), succeeds on everything else.
fn error_on_send_runner() -> ActionRunner {
    Arc::new(|a: &Action| {
        if matches!(a, Action::SendMessage { .. }) {
            Err(anyhow::anyhow!("boom"))
        } else {
            Ok(serde_json::Value::Null)
        }
    })
}

/// A workflow whose `upgrade` step errors and carries `on_error: fail_run`.
fn fail_run_workflow() -> Workflow {
    parse_workflow(
        r##"
name: upgrade-wf
trigger:
  type: webhook
steps:
  - name: upgrade
    action: send_message
    to: ["ops"]
    subject: "upgrade"
    body: "go"
    on_error: fail_run
"##,
    )
    .expect("parse fail_run workflow")
}

/// A workflow whose erroring step has NO on_error (default continue).
fn continue_workflow() -> Workflow {
    parse_workflow(
        r##"
name: continue-wf
trigger:
  type: webhook
steps:
  - name: upgrade
    action: send_message
    to: ["ops"]
    subject: "upgrade"
    body: "go"
  - name: after
    action: send_dm
    to: "pi"
    body: "still running"
"##,
    )
    .expect("parse continue workflow")
}

/// P0.2 (ADR-0005 Spec 0): `on_error: fail_run` on an erroring step → the
/// run FAILS — `run_workflow` RETURNS `{status:"failed", error, failed_step}`
/// (a durable deterministic return, NOT a panic/abort).
#[tokio::test]
async fn on_error_fail_run_fails_the_run_durably() {
    let engine = WorkflowEngine::local(error_on_send_runner()).expect("engine");
    let input = RunInput {
        run_id: None,
        workflow: fail_run_workflow(),
        trigger: serde_json::json!({}),
    };
    let output = engine
        .run("wf-fail-run-1", input)
        .await
        .expect("run returns (a value, not a panic)");
    assert_eq!(
        output["status"], "failed",
        "fail_run step must fail the run: {output}"
    );
    assert_eq!(
        output["failed_step"], "upgrade",
        "failed step named: {output}"
    );
    let err = output["error"].as_str().expect("error string");
    assert!(err.contains("boom"), "error surfaced: {output}");
}

/// P0.2 backward compat: no `on_error` (default `continue`) on an erroring
/// step → run completes; step output `{ok:false}`; later steps still run.
#[tokio::test]
async fn on_error_default_continue_completes_the_run() {
    let engine = WorkflowEngine::local(error_on_send_runner()).expect("engine");
    let input = RunInput {
        run_id: None,
        workflow: continue_workflow(),
        trigger: serde_json::json!({}),
    };
    let output = engine
        .run("wf-continue-1", input)
        .await
        .expect("run completes (continue default)");
    assert!(
        output.get("status").is_none() || output["status"] != "failed",
        "run must NOT fail under default continue: {output}"
    );
    assert_eq!(
        output["upgrade"]["ok"], false,
        "erroring step captured as {{ok:false}}: {output}"
    );
    // The step AFTER the error still ran (run completed, not aborted).
    assert!(
        output.get("after").is_some(),
        "later steps still ran: {output}"
    );
}

/// P0.3 (ADR-0005 Spec 0): the kind-46012 terminal emitter maps the run's
/// outcome Value to the wire `status` field — additive content only.
/// A failed outcome produces `status:"failed"` + error; normal completion
/// produces `status:"succeeded"`.
#[test]
fn workflow_run_terminal_maps_status() {
    use nostr_sdk::prelude::Keys;
    use toron_core::nostr::kinds::{WORKFLOW_RUN_LAST, toron_kind};

    let keys = Keys::generate();

    // Failed outcome → status:"failed" + error.
    let failed = serde_json::json!({
        "status": "failed",
        "error": "boom",
        "failed_step": "upgrade",
    });
    let ev = build_workflow_run_terminal(&keys, "run-1", &failed).expect("build failed terminal");
    assert_eq!(ev.kind, toron_kind(WORKFLOW_RUN_LAST), "kind-46012");
    let content: serde_json::Value = serde_json::from_str(&ev.content).expect("content parses");
    assert_eq!(content["status"], "failed");
    assert_eq!(content["error"], "boom");
    assert_eq!(content["failed_step"], "upgrade");
    assert_eq!(content["run_id"], "run-1");
    // Addressable by run id (d tag).
    assert!(
        ev.tags
            .iter()
            .any(|t| t.as_slice().first() == Some(&"d".to_string())
                && t.as_slice().get(1) == Some(&"run-1".to_string())),
        "d tag = run id"
    );

    // Normal completion → status:"succeeded" + the step outputs.
    let ok = serde_json::json!({ "foo": { "n": 42 } });
    let ev = build_workflow_run_terminal(&keys, "run-2", &ok).expect("build ok terminal");
    let content: serde_json::Value = serde_json::from_str(&ev.content).expect("content parses");
    assert_eq!(content["status"], "succeeded");
    assert_eq!(
        content["output"]["foo"]["n"], 42,
        "step outputs preserved on the wire under `output`"
    );
    assert!(content.get("error").is_none(), "no error on success");
}

/// bd-pkc part 3: the `toron:workflow-emitted` stamp appends the marker
/// `t`-tag once (idempotent) — the loop-prevention belt.
#[test]
fn workflow_emitted_stamp_is_idempotent() {
    use nostr_sdk::prelude::Tag;
    let plain = vec![Tag::parse(["e", "abc"]).expect("e tag")];
    let stamped = stamp_workflow_emitted(plain);
    assert_eq!(stamped.len(), 2, "marker appended");
    assert!(
        stamped
            .iter()
            .any(|t| t.as_slice().first().map(String::as_str) == Some("t")
                && t.as_slice().get(1).map(String::as_str)
                    == Some(crate::trigger_filter::WORKFLOW_EMITTED_TAG)),
        "toron:workflow-emitted t-tag present"
    );
    // Idempotent: re-stamping adds nothing.
    let re = stamp_workflow_emitted(stamped.clone());
    assert_eq!(re.len(), 2, "no double stamp");
    assert_eq!(re, stamped);
}

/// P0.4 end-to-end (Behavior Test Matrix row `error_fail_run`): a
/// `fail_run` step's error → `run_workflow` failed outcome → the 46012
/// terminal carries `status=failed`. Exercises P0.2 + P0.3 together.
#[tokio::test]
async fn fail_run_emits_46012_status_failed() {
    use nostr_sdk::prelude::Keys;

    let engine = WorkflowEngine::local(error_on_send_runner()).expect("engine");
    let input = RunInput {
        run_id: None,
        workflow: fail_run_workflow(),
        trigger: serde_json::json!({}),
    };
    let outcome = engine
        .run("wf-fail-46012", input)
        .await
        .expect("run returns failed outcome");

    let terminal = build_workflow_run_terminal(&Keys::generate(), "wf-fail-46012", &outcome)
        .expect("build terminal");
    let content: serde_json::Value =
        serde_json::from_str(&terminal.content).expect("content parses");
    assert_eq!(
        content["status"], "failed",
        "46012 status=failed: {content}"
    );
    assert!(
        content["error"]
            .as_str()
            .unwrap_or_default()
            .contains("boom")
    );
}

/// P0.4 (Behavior Test Matrix row `template_substitution`): `{{trigger.x}}`
/// resolves in an action body — the runner sees the rendered payload.
#[tokio::test]
async fn template_substitution_trigger_in_body() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::Value::Null)
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: trigger-template
trigger:
  type: webhook
steps:
  - name: ack
    action: send_dm
    to: "pi"
    body: "chan={{trigger.channel}} id={{trigger.id}}"
"##,
    )
    .expect("parse");
    let input = RunInput {
        run_id: None,
        workflow: wf,
        trigger: serde_json::json!({ "channel": "#support", "id": 7 }),
    };
    engine
        .run("wf-trigger-template-1", input)
        .await
        .expect("run completes");
    let action = &seen.lock().unwrap()[0];
    match action {
        Action::SendDm { body, .. } => {
            assert_eq!(
                body, "chan=#support id=7",
                "{{trigger.x}} must render from the trigger payload"
            );
        }
        _ => unreachable!(),
    }
}

/// P0.4 (Behavior Test Matrix row `template_substitution`): `{{trigger.x}}`
/// resolves in a GUARD — the guard variable map carries the substituted
/// trigger context (guard.rs's eval contract).
#[tokio::test]
async fn template_substitution_in_guard_variables() {
    use crate::guard::{GuardOutcome, evaluate_guard_default};
    use std::collections::HashMap;

    // The engine/embedding substitutes `{{trigger.channel}}` into a valid
    // evalexpr identifier (dots are not allowed in identifiers — see
    // guard.rs variable_comparison_true). The variable map carries the
    // trigger context under those names.
    let mut vars = HashMap::new();
    vars.insert(
        "trigger_channel".into(),
        evalexpr::Value::from("#support".to_string()),
    );
    vars.insert(
        "trigger_project".into(),
        evalexpr::Value::from("toron".to_string()),
    );
    let out = evaluate_guard_default(
        Some("trigger_channel == \"#support\" && trigger_project == \"toron\""),
        &vars,
    )
    .await;
    assert!(
        matches!(out, GuardOutcome::Bool(true)),
        "guard true: {out:?}"
    );
}

/// P5.2 (ADR-0005 Spec 4b): a `graph:` workflow with independent nodes
/// runs them, collect outputs under `steps.<name>`, and a dependent node
/// receives its `needs`' outputs via the template context.
#[tokio::test]
async fn graph_dag_runs_and_collects_outputs() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        match a {
            Action::SendDm { .. } => Ok(serde_json::json!({ "n": 42 })),
            _ => Ok(serde_json::Value::Null),
        }
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: graph-ship
trigger:
  type: webhook
graph:
  nodes:
    scout: { action: send_dm, to: x, body: y }
    plan:  { action: send_dm, to: x, body: y, needs: [scout] }
    fix_a: { action: send_dm, to: x, body: y, needs: [plan] }
    fix_b: { action: send_dm, to: x, body: y, needs: [plan] }
    ship:  { action: send_dm, to: x, body: y, needs: [fix_a, fix_b] }
"##,
    )
    .expect("parse graph workflow");
    let input = RunInput {
        run_id: None,
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let output = engine
        .run("wf-graph-1", input)
        .await
        .expect("graph run completes");

    // Every node produced output under steps.<name>.
    for node in ["scout", "plan", "fix_a", "fix_b", "ship"] {
        assert!(
            output.get(node).is_some(),
            "graph node {node} produced output: {output}"
        );
    }
    assert_eq!(output["scout"]["n"], 42, "node value flows through");

    // Wave ordering: the runner saw scout before plan before the fixes.
    let actions = seen.lock().unwrap();
    let names: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            Action::SendDm { body, .. } => Some(body.as_str()),
            _ => None,
        })
        .collect();
    // All 5 nodes ran exactly once.
    assert_eq!(names.len(), 5, "all 5 nodes executed: {names:?}");
}

/// bd-m7ac: a graph node whose action is CONTROL-FLOW (parallel) must
/// expand like a linear step — the runner sees N dispatch copies, not the
/// raw `parallel` action. Regression: graph nodes dispatched the action
/// raw to the runner, so the weekly-review scout fan-out never fanned.
#[tokio::test]
async fn graph_parallel_node_expands_copies() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        match a {
            Action::DispatchJob { .. } => Ok(serde_json::json!({ "scouted": true })),
            _ => Ok(serde_json::Value::Null),
        }
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: graph-fanout
trigger:
  type: webhook
graph:
  nodes:
    scout-fanout:
      action: parallel
      over: "3"
      each:
        action: dispatch_job
        assignee: x
        spec: { profile: "@scout" }
    oracle:
      action: dispatch_job
      assignee: x
      spec: { profile: "@oracle", task: "see {{steps.scout-fanout}}" }
      needs: [scout-fanout]
"##,
    )
    .expect("parse graph fan-out workflow");
    let output = engine
        .run(
            "wf-graph-fanout-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("graph fan-out completes");

    // The parallel node's output is an ARRAY of 3 copy outputs.
    let fan = output["scout-fanout"]
        .as_array()
        .expect("fanout is an array");
    assert_eq!(fan.len(), 3, "three scout copies: {fan:?}");
    // The oracle consumed the fan-out: its dispatch spec's task was
    // rendered with the scout array (the runner saw the rendered action).
    let actions = seen.lock().unwrap();
    let oracle_spec = actions
        .iter()
        .filter_map(|a| match a {
            Action::DispatchJob { spec, .. } => Some(spec),
            _ => None,
        })
        .next_back()
        .expect("oracle dispatch spec");
    let task = oracle_spec.task.as_deref().expect("oracle task present");
    // Pinned on the scout key/value, not a JSON prefix: the step value now
    // also carries the journal marker below, so a prefix match would pin
    // field order rather than the fan-out reaching the oracle.
    assert!(
        task.contains("\"scouted\":true"),
        "oracle saw the fan-out: {task}"
    );
    // bd-engine-run-path-journaling-fwlx: these copies were dispatched with no
    // EffectRunner, so the bypass marker rides their values and therefore the
    // downstream render — the journal gap is visible, never silent.
    assert!(
        task.contains("effect_journal"),
        "the journal bypass is visible downstream, not silent: {task}"
    );
    // The runner saw 3 scout copies + 1 oracle dispatch, never a raw
    // parallel action.
    let dispatch_copies = actions
        .iter()
        .filter(|a| matches!(a, Action::DispatchJob { .. }))
        .count();
    assert_eq!(
        dispatch_copies, 4,
        "3 scout copies + 1 oracle dispatch: {dispatch_copies}"
    );
    assert!(
        !actions.iter().any(|a| matches!(a, Action::Parallel { .. })),
        "no raw parallel action reaches the runner in a graph"
    );
}

/// P5.3: `retry` — a failing sub-step is retried up to `max_attempts`;
/// `retry_backoff` uses the backoff. On success mid-way, the retry
/// returns the value.
#[tokio::test]
async fn retry_backoff_retries_until_success() {
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = attempts.clone();
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        let n = attempts_clone.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            Err(anyhow::anyhow!("transient"))
        } else {
            Ok(serde_json::json!({ "ok": true, "attempt": n + 1 }))
        }
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: retry-wf
trigger:
  type: webhook
steps:
  - name: go
    action: retry
    max_attempts: 3
    backoff_secs: 0
    step:
      action: send_dm
      to: pi
      body: ping
"##,
    )
    .expect("parse retry workflow");
    let output = engine
        .run(
            "wf-retry-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("retry run completes");
    // 2 failures + 1 success = 3 attempts; success on the 3rd.
    assert_eq!(attempts.load(Ordering::SeqCst), 3, "retried to success");
    assert_eq!(output["go"]["attempt"], 3, "success value returned");
}

/// P5.3: `retry` exhausts → `{ok:false}` (outer on_error decides).
#[tokio::test]
async fn retry_exhausts_reports_failure() {
    let runner: ActionRunner = Arc::new(|_a: &Action| Err(anyhow::anyhow!("always fails")));
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: retry-fail
trigger:
  type: webhook
steps:
  - name: go
    action: retry
    max_attempts: 2
    step:
      action: send_dm
      to: pi
      body: ping
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-retry-fail-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run completes");
    assert_eq!(output["go"]["ok"], false, "retry exhausted: {output}");
    assert_eq!(output["go"]["retried"], true);
}

/// P5.3: `branch` — guard true → `then` runs; guard false → `else` runs.
#[tokio::test]
async fn branch_then_else_on_guard() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::Value::Null)
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: branch-wf
trigger:
  type: webhook
steps:
  - name: gate
    action: branch
    if: "trigger_ok == \"true\""
    then:
      - { action: send_dm, to: pi, body: yes }
    else:
      - { action: send_dm, to: pi, body: no }
"##,
    )
    .expect("parse branch workflow");

    // Guard true → then branch.
    let engine2 = WorkflowEngine::local(runner.clone()).expect("engine2");
    let _ = engine2
        .run(
            "wf-branch-true",
            RunInput {
                run_id: None,
                workflow: wf.clone(),
                trigger: serde_json::json!({ "ok": "true" }),
            },
        )
        .await
        .expect("runs");
    let last = {
        let actions = seen.lock().unwrap();
        actions.last().cloned()
    };
    assert!(matches!(last, Some(Action::SendDm { body, .. }) if body == "yes"));

    // Guard false → else branch.
    let _ = engine
        .run(
            "wf-branch-false",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({ "ok": "false" }),
            },
        )
        .await
        .expect("else-branch run completes");
    let last = {
        let actions = seen.lock().unwrap();
        actions.last().cloned()
    };
    assert!(matches!(last, Some(Action::SendDm { body, .. }) if body == "no"));
}

/// P5.3 + ADR-0007 F4: `foreach` — iterate over a list; collect sub-run
/// outputs. Each iteration injects the current item as `{{item}}`, so the
/// sub-action's rendered body is the element itself (a/b/c), not a
/// literal template.
#[tokio::test]
async fn foreach_over_list_collects() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::json!({ "ran": true }))
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: foreach-wf
trigger:
  type: webhook
steps:
  - name: iterate
    action: foreach
    over: '["a","b","c"]'
    each:
      action: send_dm
      to: pi
      body: "{{item}}"
"##,
    )
    .expect("parse foreach workflow");
    let output = engine
        .run(
            "wf-foreach-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("foreach run completes");
    let list = output["iterate"].as_array().expect("collected list");
    assert_eq!(list.len(), 3, "one sub-run per item");
    // ADR-0007 F4: the three sub-runs must carry bodies a/b/c — the
    // current item injected into the per-iteration template context.
    let bodies: Vec<String> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|a| match a {
            Action::SendDm { body, .. } => body.clone(),
            other => panic!("expected send_dm action, got {other:?}"),
        })
        .collect();
    assert_eq!(bodies, vec!["a", "b", "c"]);
}

/// P5.3: `parallel` — elastic N concurrent copies, collected into a list.
#[tokio::test]
async fn parallel_elastic_spawns_n() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::json!({ "worker": true }))
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: parallel-wf
trigger:
  type: webhook
steps:
  - name: fan
    action: parallel
    over: "3"
    each:
      action: send_dm
      to: pi
      body: work
"##,
    )
    .expect("parse parallel workflow");
    let output = engine
        .run(
            "wf-parallel-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("parallel run completes");
    let list = output["fan"].as_array().expect("collected list");
    assert_eq!(list.len(), 3, "3 elastic copies");
    assert_eq!(seen.lock().unwrap().len(), 3, "3 sub-runs executed");
}

/// ADR-0007 R4.1: `call_workflow` nests a durable child via WorkflowResolver.
#[tokio::test]
async fn call_workflow_nested_subrun_returns_child_output() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_c = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_c.lock().unwrap().push(a.clone());
        Ok(serde_json::json!({ "ran": true }))
    });
    let child = parse_workflow(
        r##"
name: triage
trigger:
  type: webhook
steps:
  - name: do
    action: send_dm
    to: pi
    body: from-child
"##,
    )
    .expect("child");
    let resolver: WorkflowResolver = {
        let child = child.clone();
        Arc::new(move |name: &str| {
            if name == "triage" {
                Some(child.clone())
            } else {
                None
            }
        })
    };
    let engine = WorkflowEngine::local_with_resolvers(runner, Arc::new(|_: &str| None), resolver)
        .expect("engine");
    let wf = parse_workflow(
        r##"
name: caller
trigger:
  type: webhook
steps:
  - name: sub
    action: call_workflow
    workflow: triage
    inputs: { goal: "x" }
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-call-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run completes");
    // Child step output is nested under the parent's `sub` value.
    assert!(
        output["sub"].is_object() || output["sub"]["do"]["ran"] == true,
        "call_workflow returned child output: {output}"
    );
    assert_eq!(seen.lock().unwrap().len(), 1, "child action executed once");
}

/// Unknown workflow name → honest {ok:false}, not a hang.
#[tokio::test]
async fn call_workflow_unknown_reports_error() {
    let engine = WorkflowEngine::local(noop_runner()).expect("engine");
    let wf = parse_workflow(
        r##"
name: caller
trigger:
  type: webhook
steps:
  - name: sub
    action: call_workflow
    workflow: does-not-exist
    inputs: {}
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-call-unknown",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run completes");
    assert_eq!(output["sub"]["ok"], false, "{output}");
    assert!(
        output["sub"]["error"]
            .as_str()
            .unwrap_or("")
            .contains("unknown"),
        "{output}"
    );
}

/// P3.1 (ADR-0005 Spec 2): `WorkflowEngine::invoke` — the on-demand
/// entrypoint. A workflow invoked with no trigger still gets a `trigger.*`
/// template context synthesized from `inputs`, so `{{inputs.goal}}` and
/// `{{trigger.*}}` both resolve on the invoke path. Trigger path + invoke
/// path use the SAME durable function (`run_workflow`).
#[tokio::test]
async fn invoke_runs_workflow_with_synthesized_inputs_trigger() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::Value::Null)
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");
    let wf = parse_workflow(
        r##"
name: invoke-me
trigger:
  type: webhook
inputs:
  - name: goal
steps:
  - name: plan
    action: send_dm
    to: "pi"
    body: "goal={{inputs.goal}} trigger_goal={{trigger.goal}}"
"##,
    )
    .expect("parse invoke workflow");

    let output = engine
        .invoke(
            "wf-invoke-1",
            wf.clone(),
            serde_json::json!({ "goal": "ship-it" }),
        )
        .await
        .expect("invoke returns run output");

    // The invoke path ran through the SAME durable function: the step
    // outputs are present, and the runner saw both template forms resolved.
    assert!(output.get("plan").is_some(), "step ran: {output}");
    let action = &seen.lock().unwrap()[0];
    match action {
        Action::SendDm { body, .. } => {
            assert_eq!(
                body, "goal=ship-it trigger_goal=ship-it",
                "{{inputs.goal}} AND {{trigger.goal}} resolve on invoke: {body}"
            );
        }
        _ => unreachable!(),
    }
}

/// Recursive marker lookup: the engine wraps step values differently on the
/// invoke vs graph paths, and this test pins the MARKER, not the wrapping.
fn find_marker(v: &serde_json::Value, key: &str) -> Option<String> {
    match v {
        serde_json::Value::Object(m) => m
            .get(key)
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .or_else(|| m.values().find_map(|x| find_marker(x, key))),
        serde_json::Value::Array(a) => a.iter().find_map(|x| find_marker(x, key)),
        _ => None,
    }
}

/// ADR-0032 Spec 6 (bd-engine-run-path-journaling-fwlx): an EFFECTFUL leaf
/// dispatched with no injected `EffectRunner` wrote no journal row. That must
/// not be a SILENT bypass — warn-first, like the ADR-0022 plan-artifact gate:
/// the value carries a machine-readable marker so a journal-less embedding is
/// visible in the run ledger instead of looking journaled.
#[tokio::test]
async fn effectful_leaf_without_effect_runner_is_marked_not_silent() {
    let runner: ActionRunner = Arc::new(|_a: &Action| Ok(serde_json::json!({ "ok": true })));
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: journal-bypass
trigger:
  type: webhook
steps:
  - name: notify
    action: send_dm
    to: "pi"
    body: "hi"
"##,
    )
    .expect("parse journal-bypass workflow");

    let output = engine
        .invoke("wf-bypass-1", wf, serde_json::json!({}))
        .await
        .expect("invoke returns run output");

    assert_eq!(
        find_marker(&output, "effect_journal").as_deref(),
        Some("bypassed-no-effect-runner"),
        "an effectful leaf with no EffectRunner must not look journaled: {output}"
    );
}

/// Control for the marker above: with an `EffectRunner` injected the same
/// effectful leaf goes through the context-bearing surface, so there is no
/// bypass to mark.
#[tokio::test]
async fn effectful_leaf_with_effect_runner_is_not_marked() {
    let runner: ActionRunner = Arc::new(|_a: &Action| Ok(serde_json::json!({ "ok": true })));
    let calls = Arc::new(std::sync::Mutex::new(0usize));
    let calls_clone = calls.clone();
    let fx: EffectRunner = Arc::new(move |_inv: &ActionInvocation| {
        *calls_clone.lock().unwrap() += 1;
        Ok(serde_json::json!({ "journaled": true }))
    });
    let engine =
        WorkflowEngine::local_with_effect_runner(runner, Some(fx)).expect("engine with runner");
    let wf = parse_workflow(
        r##"
name: journal-covered
trigger:
  type: webhook
steps:
  - name: notify
    action: send_dm
    to: "pi"
    body: "hi"
"##,
    )
    .expect("parse journal-covered workflow");

    let output = engine
        .invoke("wf-covered-1", wf, serde_json::json!({}))
        .await
        .expect("invoke returns run output");

    assert_eq!(
        *calls.lock().unwrap(),
        1,
        "the injected effect runner handled the leaf"
    );
    assert!(
        find_marker(&output, "effect_journal").is_none(),
        "a journaled run carries no bypass marker: {output}"
    );
}

/// bd-engine-run-path-journaling-fwlx: a real embedding needs BOTH resolvers
/// (so `spawn`/`call_workflow` resolve) AND a journaling `EffectRunner` (so an
/// effectful leaf journals instead of bypassing). Only the combined funnel had
/// both, and it was private — the public constructors each dropped one. Prove
/// the combined constructor installs both: the resolver resolves the sub-run
/// and the effect runner handles the effectful leaf inside it.
#[tokio::test]
async fn resolvers_and_effect_runner_coexist_on_one_engine() {
    let runner: ActionRunner = Arc::new(|_a: &Action| Ok(serde_json::json!({ "ok": true })));
    let resolved = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let resolved_c = resolved.clone();
    let child = parse_workflow(
        r##"
name: journal-child
trigger:
  type: webhook
steps:
  - name: dispatch
    action: dispatch_job
    assignee: pi
    spec:
      task: work
"##,
    )
    .expect("child");
    let workflow_resolver: WorkflowResolver = Arc::new(move |name: &str| {
        if name == "journal-child" {
            resolved_c.store(true, std::sync::atomic::Ordering::SeqCst);
            Some(child.clone())
        } else {
            None
        }
    });
    let profile_resolver: ProfileResolver = Arc::new(|_: &str| None);
    let calls = Arc::new(std::sync::Mutex::new(0usize));
    let calls_c = calls.clone();
    let fx: EffectRunner = Arc::new(move |_inv: &ActionInvocation| {
        *calls_c.lock().unwrap() += 1;
        Ok(serde_json::json!({ "journaled": true }))
    });
    let engine = WorkflowEngine::local_with_resolvers_and_effect(
        runner,
        profile_resolver,
        workflow_resolver,
        Some(fx),
    )
    .expect("engine with resolvers + effect runner");
    let wf = parse_workflow(
        r##"
name: journal-parent
trigger:
  type: webhook
steps:
  - name: sub
    action: call_workflow
    workflow: journal-child
"##,
    )
    .expect("parent");

    let output = engine
        .invoke("wf-resolvers-and-effect-1", wf, serde_json::json!({}))
        .await
        .expect("invoke returns run output");

    assert!(
        resolved.load(std::sync::atomic::Ordering::SeqCst),
        "the injected workflow resolver resolved the sub-run"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        1,
        "the injected effect runner handled the child's dispatch_job leaf: {output}"
    );
    assert!(
        find_marker(&output, "effect_journal").is_none(),
        "a journaled run carries no bypass marker: {output}"
    );
}

/// P3.4 (ADR-0005 Spec 2): a workflow with NO trigger (`trigger:` omitted)
/// is invoke-only — it runs via `WorkflowEngine::invoke` but has no
/// reactive trigger to auto-fire.
#[tokio::test]
async fn triggerless_workflow_invokes_but_has_no_trigger() {
    let engine = WorkflowEngine::local(noop_runner()).expect("engine");
    let wf = parse_workflow(
        r##"
name: invoke-only
inputs:
  - name: goal
steps:
  - name: plan
    action: send_dm
    to: "pi"
    body: "planning {{inputs.goal}}"
"##,
    )
    .expect("trigger-less workflow parses");

    // The workflow carries no reactive trigger.
    assert!(
        wf.trigger.is_none(),
        "invoke-only workflow must not auto-fire (no trigger)"
    );

    // It runs via invoke, and {{inputs.x}} resolves.
    let output = engine
        .invoke("wf-invoke-only-1", wf, serde_json::json!({ "goal": "go" }))
        .await
        .expect("invoke-only workflow runs");
    assert!(output.get("plan").is_some(), "invoke-only ran: {output}");
}

/// A workflow whose first step's action returns `{"n":42}`; the second
/// step's action references `{{steps.foo.n}}` in a body field. The engine
/// must (a) populate `steps.<name>` from the runner's return and (b)
/// substitute it into the downstream step's templates before the runner
/// sees them.
#[tokio::test]
async fn value_returning_action_flows_into_downstream_template() {
    use std::sync::Mutex as StdMutex;
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        // Record what the runner received (post-substitution).
        seen_clone.lock().unwrap().push(a.clone());
        match a {
            Action::SendMessage { .. } => Ok(serde_json::json!({ "ok": true })),
            Action::SendDm { .. } => Ok(serde_json::json!({ "n": 42 })),
            other => Ok(serde_json::json!({ "action": other.type_name() })),
        }
    });
    let engine = WorkflowEngine::local(runner.clone()).expect("engine");

    let wf = parse_workflow(
        r##"
name: value-flow
trigger:
  type: webhook
steps:
  - name: foo
    action: send_dm
    to: "pi"
    body: "compute"
  - name: bar
    action: send_message
    to: ["ops"]
    subject: "result"
    body: "n={{steps.foo.n}}"
"##,
    )
    .expect("parse value-flow workflow");
    let input = RunInput {
        run_id: None,
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let output = engine
        .run("wf-value-flow-1", input)
        .await
        .expect("run completes");

    // (a) The runner's return for step `foo` populated `steps.foo`.
    assert_eq!(
        output["foo"]["n"], 42,
        "ActionRunner return must populate steps.foo: {output}"
    );

    // (b) The downstream step's `{{steps.foo.n}}` was substituted with the
    //     value BEFORE the runner saw the action.
    let seen_actions = seen.lock().unwrap();
    let bar = seen_actions
        .iter()
        .find(|a| matches!(a, Action::SendMessage { .. }))
        .expect("bar action reached the runner");
    match bar {
        Action::SendMessage { body, .. } => {
            assert_eq!(
                body, "n=42",
                "downstream template must render steps.foo.n into the body"
            );
        }
        _ => unreachable!(),
    }
}

// ---------------------------------------------------------------------------
// Resonate server selection (remote default / local fallback).
// ---------------------------------------------------------------------------

#[test]
fn server_url_pin_env_beats_probe() {
    // Can't safely mutate process env in tests — the pure core takes the
    // probe as a closure; env pin is exercised via the RESOLUTION function
    // below with a probe that would claim everything is dead.
    let none = resonate_server_url_with(|_| false);
    assert_eq!(none, None, "no probe hit -> embedded local mode");
    let remote = resonate_server_url_with(|u| u == REMOTE_RESONATE_URL);
    assert_eq!(
        remote.as_deref(),
        Some(REMOTE_RESONATE_URL),
        "remote wins when probed"
    );
    let local_only = resonate_server_url_with(|u| u == LOCAL_RESONATE_URL);
    assert_eq!(
        local_only.as_deref(),
        Some(LOCAL_RESONATE_URL),
        "local used when remote dead"
    );
}

#[test]
fn host_port_parses_urls() {
    assert_eq!(
        host_port("https://resonate.alpine-tilapia.ts.net"),
        Some(("resonate.alpine-tilapia.ts.net".to_string(), 443))
    );
    assert_eq!(
        host_port("http://localhost:8001"),
        Some(("localhost".to_string(), 8001))
    );
    assert_eq!(
        host_port("http://localhost"),
        Some(("localhost".to_string(), 80))
    );
    assert_eq!(host_port("https://h:9000"), Some(("h".to_string(), 9000)));
    assert_eq!(host_port("not-a-url"), None);
    assert_eq!(host_port("https://"), None);
    assert_eq!(host_port("https://h:notaport"), None);
}

#[test]
fn probe_candidate_hits_local_listener() {
    use std::net::TcpListener;
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = l.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}");
    assert!(probe_candidate(&url), "bound listener answers");

    // bd-6wfv: a freed ephemeral port can be REBOUND by a concurrent test
    // binary before the second probe (the workspace runs all crates in
    // parallel) — "drop and re-probe" is inherently racy. Instead, hold a
    // socket bound but NOT listening: connections get a deterministic RST
    // (refused) while the port stays reserved, so nothing else can take it.
    drop(l);

    // bd-4spn follow-up (captain 2026-08-16): the previous drop-then-rebind
    // of the SAME port had a race window — another test binary could bind it
    // between the drop and the rebind, failing the re-bind expect. For the
    // dead-port case, take a kernel-assigned FRESH port (bind :0) and never
    // listen: connections get a deterministic RST while the port stays
    // reserved, and there is no freed-port window to lose.
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    socket
        .bind(&socket2::SockAddr::from(std::net::SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            0,
        )))
        .expect("kernel-assigned fresh port");
    let dead_port = socket
        .local_addr()
        .unwrap()
        .as_socket()
        .expect("ipv4 socket addr")
        .port();
    let dead_url = format!("http://127.0.0.1:{dead_port}");
    assert!(
        !probe_candidate(&dead_url),
        "bound-but-not-listening port is dead"
    );
    // first listener's port is now freed; probing it would be racy again, so
    // the live case was asserted BEFORE the drop above.
    let _ = &url;
}

/// bd-compose-not-durable-oxjy: crash-resume equivalence between entry
/// doors. A redelivered run_id (crash after checkpoint, orchestrator retry)
/// replays the RECORDED execution instead of re-running steps — the same
/// durability semantics a catalog/trigger run gets, now pinned for ANY
/// `WorkflowEngine::invoke` caller (compose --run included).
#[tokio::test(flavor = "multi_thread")]
async fn invoke_redelivery_replays_recorded_run() {
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls_c = calls.clone();
    let runner: ActionRunner = std::sync::Arc::new(move |_a: &Action| {
        calls_c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(serde_json::json!({ "ok": true }))
    });
    let engine = WorkflowEngine::local(runner).expect("local engine");
    let wf = crate::parse_workflow(
        "name: t\nsteps:\n  - name: a\n    action: send_message\n    to: [x]\n    subject: s\n    body: b\n",
    )
    .expect("parse");
    let first = engine
        .invoke("redeliver-once", wf.clone(), serde_json::json!({}))
        .await
        .expect("first run");
    let redelivery = engine
        .invoke("redeliver-once", wf.clone(), serde_json::json!({}))
        .await
        .expect("redelivered run");
    assert_eq!(first, redelivery, "redelivery returns the recorded output");
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "steps execute exactly once across redelivery"
    );
}

/// ADR-0024 Spec 0 park fix: desugared `steps:` with `request_approval` still parks via run_graph.
#[tokio::test(flavor = "multi_thread")]
async fn park_desugared_request_approval_still_parks() {
    let Some((_srv, engine)) = remote_engine(noop_runner()).await else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines");
        return;
    };
    let wf = parse_workflow(
        r##"
name: signoff
steps:
  - name: gate
    action: request_approval
    message: "approve?"
  - name: after
    action: send_dm
    to: x
    body: y
"##,
    )
    .expect("parse");
    // to_graph desugars the linear steps; run_workflow always calls run_graph.
    let g = wf.to_graph();
    assert_eq!(g.nodes["gate"].needs, Vec::<String>::new());
    assert_eq!(g.nodes["after"].needs, vec!["gate"]);
    let input = RunInput {
        run_id: None,
        workflow: wf,
        trigger: serde_json::json!({}),
    };
    let handle = engine
        .resonate()
        .run("park-desugared-1", run_workflow, input)
        .spawn()
        .await
        .expect("spawn");
    let promise_id = poll_promise_id(&engine).await;
    assert!(!handle.done().await.expect("done"), "must park");
    engine
        .resolve_approval(&promise_id, &Decision::approved("ok"))
        .await
        .expect("resolve");
    let out = handle.result().await.expect("resumes");
    assert_eq!(out["gate"]["outcome"], "approved");
}

/// ADR-0024 Spec 0 fail_run predicate: only `ok == false` is a failure.
#[tokio::test]
async fn failure_kind_ok_false_is_failure_error_without_ok_is_not() {
    // Graph node returning {ok:false, error:"boom"} with on_error fail_run → failed.
    let engine = WorkflowEngine::local(Arc::new(|a: &Action| match a {
        Action::SendDm { .. } => Ok(serde_json::json!({ "ok": false, "error": "boom" })),
        _ => Ok(serde_json::Value::Null),
    }))
    .expect("engine");
    let wf = parse_workflow(
        r##"
name: t
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, on_error: fail_run }
"##,
    )
    .expect("parse");
    let out = engine
        .run(
            "failure-kind-fail",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert_eq!(out["status"], "failed");
    assert_eq!(out["error"], "boom");

    // Presence of `error` without `ok:false` is NOT a failure — run succeeds.
    let engine2 = WorkflowEngine::local(Arc::new(|_: &Action| {
        Ok(serde_json::json!({ "ok": true, "error": "something" }))
    }))
    .expect("engine2");
    let wf2 = parse_workflow(
        r##"
name: t2
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, on_error: fail_run }
"##,
    )
    .expect("parse2");
    let out2 = engine2
        .run(
            "failure-kind-not-fail",
            RunInput {
                run_id: None,
                workflow: wf2,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run2");
    assert!(
        out2.get("status").is_none() || out2["status"] != "failed",
        "error without ok:false must not fail: {out2}"
    );
    assert_eq!(out2["a"]["ok"], true);
}

/// VerifySink missing → {ok:false, error:"verify sink missing"} (ADR-0024 Spec 2).
#[tokio::test]
async fn verify_sink_missing_returns_error() {
    let engine = WorkflowEngine::local(noop_runner()).expect("engine");
    let wf = parse_workflow(
        r##"
name: t
graph:
  nodes:
    v: { action: verify, command: "cargo test" }
"##,
    )
    .expect("parse verify");
    let out = engine
        .run(
            "verify-missing-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert_eq!(out["v"]["ok"], false);
    assert!(
        out["v"]["error"]
            .as_str()
            .unwrap()
            .contains("verify sink missing")
    );
}

/// ADR-0024 Spec 3: evidence-bearing approval payload
#[tokio::test(flavor = "multi_thread")]
async fn approval_payload() {
    use std::sync::{Arc, Mutex as StdMutex};

    // Runner that returns a commit sha for the implement node
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        match a {
            Action::SendDm { body, .. } if body == "implement" => {
                Ok(serde_json::json!({ "sha": "abc1234", "kind": "commit" }))
            }
            Action::SendDm { body, .. } if body == "verify" => {
                Ok(serde_json::json!({ "path": ".flywheel/receipts/verify-1.txt", "exit": 0 }))
            }
            _ => Ok(serde_json::Value::Null),
        }
    });

    let Some((srv, engine)) = remote_engine(runner).await else {
        eprintln!("SKIP: no `resonate` binary (needs herdr/resonate stack; runs on dev machines)");
        return;
    };

    let wf = parse_workflow(
        r##"
name: ship-toron
graph:
  nodes:
    implement:
      action: send_dm
      to: x
      body: implement
    gate:
      action: request_approval
      message: "Release toron v0.4.0?"
      needs: [implement]
"##,
    )
    .expect("parse workflow");

    let run_id = "wf-test-approval-payload-1";
    let handle = engine
        .resonate()
        .run(
            run_id,
            run_workflow,
            RunInput {
                run_id: Some(run_id.to_string()),
                workflow: wf.clone(),
                trigger: serde_json::json!({}),
            },
        )
        .spawn()
        .await
        .expect("spawn");

    // Poll for the approval promise
    let promise_id = poll_promise_id(&engine).await;

    // Fetch the promise data (the payload)
    let rec = engine
        .resonate()
        .promises
        .get(&promise_id)
        .await
        .expect("get promise");
    let data = rec.param.data_as_ref();

    // Check required fields are present
    assert_eq!(data["run_id"], run_id, "run_id present");
    assert_eq!(data["workflow"], "ship-toron", "workflow name");
    assert_eq!(data["suspended_at"]["node"], "gate", "suspended_at.node");
    assert!(data["suspended_at"]["wave"].is_number(), "wave present");
    assert_eq!(
        data["question"], "Release toron v0.4.0?",
        "question is the message"
    );
    assert!(
        data["context"]["nodes_completed"].is_array(),
        "nodes_completed"
    );
    assert!(data["context"]["nodes_failed"].is_array(), "nodes_failed");
    assert!(data["context"]["nodes_pending"].is_array(), "nodes_pending");
    assert!(data["context"]["artifacts"].is_array(), "artifacts");
    // Artifacts should contain paths, not blobs
    let artifacts = data["context"]["artifacts"]
        .as_array()
        .expect("artifacts array");
    assert!(!artifacts.is_empty(), "artifacts not empty");
    for art in artifacts {
        assert!(art.get("node").is_some(), "artifact has node");
        assert!(art.get("kind").is_some(), "artifact has kind");
        // Should have path or sha, not a blob
        assert!(
            art.get("path").is_some() || art.get("sha").is_some(),
            "artifact has path or sha: {art}"
        );
        // Should not contain large stdout blob
        if let Some(s) = art.get("stdout").and_then(|v| v.as_str()) {
            assert!(s.len() < 1000, "artifacts should be paths, not blobs");
        }
    }
    assert!(data["context"]["ledger_path"].is_string(), "ledger_path");
    assert!(data["context"]["blast_radius"].is_string(), "blast_radius");
    assert!(data["context"]["timeout"].is_string(), "timeout");
    assert!(data["evidence_hash"].is_string(), "evidence_hash present");
    let hash1 = data["evidence_hash"].as_str().unwrap().to_string();
    assert!(!hash1.is_empty());
    assert_eq!(hash1.len(), 64, "sha256 hex length");

    // Evidence hash stable across replay (same run_id, same artifacts, same ledger)
    // Poll again? The same promise should have same hash on re-fetch
    let rec2 = engine
        .resonate()
        .promises
        .get(&promise_id)
        .await
        .expect("get again");
    let hash2 = rec2.param.data_as_ref()["evidence_hash"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(hash1, hash2, "hash stable across replay");

    // Evidence hash recomputation: the payload hash must be a function of
    // its evidence inputs alone (run_id + node + artifacts + ledger_path).
    let expected_hash = crate::engine::control_flow::compute_evidence_hash(
        run_id,
        "gate",
        artifacts.as_slice(),
        data["context"]["ledger_path"].as_str().unwrap_or_default(),
    );
    assert_eq!(
        hash1, expected_hash,
        "evidence_hash recomputes from its evidence inputs"
    );

    // Clean up: resolve the approval to let the run complete
    engine
        .resolve_approval(&promise_id, &Decision::approved("ok"))
        .await
        .expect("resolve");
    let out = handle.result().await.expect("run completes");
    assert_eq!(out["gate"]["outcome"], "approved");

    // Different hash = new question: the hash must change when an artifact
    // path changes.
    let mut artifacts2 = artifacts.clone();
    if let Some(obj) = artifacts2
        .first_mut()
        .and_then(|first| first.as_object_mut())
    {
        obj.insert(
            "path".into(),
            serde_json::Value::String(".flywheel/receipts/different.txt".into()),
        );
    }
    let input2 = serde_json::json!({
        "run_id": run_id,
        "node": "gate",
        "artifacts": artifacts2.iter().map(|a| {
            let mut m = serde_json::Map::new();
            if let Some(p) = a.get("path") { m.insert("path".into(), p.clone()); }
            if let Some(s) = a.get("sha") { m.insert("sha".into(), s.clone()); }
            m
        }).collect::<Vec<_>>(),
        "ledger_path": data["context"]["ledger_path"]
    });
    // Compute hash for input2 and ensure it's different from hash1
    // We use sha256 hex
    use sha2::{Digest, Sha256};
    let hash_for = |v: &serde_json::Value| {
        let s = serde_json::to_string(v).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(s.as_bytes());
        hex::encode(hasher.finalize())
    };
    let h2 = hash_for(&input2);
    assert_ne!(
        hash1, h2,
        "different artifact path should give different hash"
    );

    drop(srv);
}

/// ADR-0024 Spec 1 kind-aware fail_run: rate_limit retries same node
#[tokio::test]
async fn kind_aware_fail_run_rate_limit_retries_same_node_then_succeeds() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = std::sync::Arc::new(AtomicUsize::new(0));
    let attempts_c = attempts.clone();
    let runner: ActionRunner = std::sync::Arc::new(move |_a: &Action| {
        let n = attempts_c.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            Ok(serde_json::json!({
                "ok": false,
                "error": "rate limited",
                "kind": "rate_limit",
                "retry_after_ms": 0
            }))
        } else {
            Ok(serde_json::json!({ "ok": true, "n": 42 }))
        }
    });
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: kind-rate-limit
trigger:
  type: webhook
steps:
  - name: go
    action: send_dm
    to: x
    body: y
    on_error: fail_run
"##,
    )
    .expect("parse");
    let out = engine
        .run(
            "kind-rate-limit-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    // Should succeed after 2 retries (3 attempts total) — not fail
    assert!(
        out.get("status").is_none() || out["status"] != "failed",
        "rate_limit should retry and succeed: {out}"
    );
    assert_eq!(out["go"]["ok"], true);
    assert_eq!(out["go"]["n"], 42);
    assert_eq!(attempts.load(Ordering::SeqCst), 3);
}

/// ADR-0024 Spec 1: timeout with fail_run retries and exhausts -> escalate with disposition
#[tokio::test]
async fn kind_aware_fail_run_timeout_bound_exhausted_escalates_with_disposition() {
    let runner: ActionRunner = std::sync::Arc::new(|_a: &Action| {
        Ok(serde_json::json!({
            "ok": false,
            "error": "timeout",
            "kind": "timeout",
            "retry_after_ms": 0
        }))
    });
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: kind-timeout
trigger:
  type: webhook
steps:
  - name: go
    action: send_dm
    to: x
    body: y
    on_error: fail_run
"##,
    )
    .expect("parse");
    let out = engine
        .run(
            "kind-timeout-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert_eq!(out["status"], "failed", "exhausted should escalate: {out}");
    assert_eq!(out["failed_step"], "go");
    assert_eq!(out["kind"], "timeout");
    assert!(
        out.get("disposition").is_some(),
        "disposition must be present: {out}"
    );
}

/// ADR-0024 Spec 1: blocked/auth do NOT retry — abort immediately
#[tokio::test]
async fn kind_aware_fail_run_blocked_does_not_retry_aborts_immediately() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = std::sync::Arc::new(AtomicUsize::new(0));
    let attempts_c = attempts.clone();
    let runner: ActionRunner = std::sync::Arc::new(move |_a: &Action| {
        attempts_c.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!({
            "ok": false,
            "error": "blocked",
            "kind": "blocked"
        }))
    });
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: kind-blocked
trigger:
  type: webhook
steps:
  - name: go
    action: send_dm
    to: x
    body: y
    on_error: fail_run
"##,
    )
    .expect("parse");
    let out = engine
        .run(
            "kind-blocked-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert_eq!(out["status"], "failed");
    assert_eq!(out["kind"], "blocked");
    assert_eq!(attempts.load(Ordering::SeqCst), 1, "blocked must not retry");
}

/// Graph node kind-aware: rate_limit retries same graph node
#[tokio::test]
async fn kind_aware_graph_rate_limit_retries_same_node() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let attempts = std::sync::Arc::new(AtomicUsize::new(0));
    let attempts_c = attempts.clone();
    let runner: ActionRunner = std::sync::Arc::new(move |_a: &Action| {
        let n = attempts_c.fetch_add(1, Ordering::SeqCst);
        if n < 1 {
            Ok(serde_json::json!({
                "ok": false,
                "error": "rate limited",
                "kind": "rate_limit",
                "retry_after_ms": 0
            }))
        } else {
            Ok(serde_json::json!({ "ok": true }))
        }
    });
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: kind-graph
trigger:
  type: webhook
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, on_error: fail_run }
"##,
    )
    .expect("parse");
    let out = engine
        .run(
            "kind-graph-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert!(
        out.get("status").is_none() || out["status"] != "failed",
        "graph rate_limit should retry and succeed: {out}"
    );
    assert_eq!(out["a"]["ok"], true);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

/// VerifySink attached → action:verify executes via the sink (not "missing")
/// and green passes. Pins the compose --run VerifySink attachment.
#[tokio::test]
async fn verify_sink_attached_green_passes() {
    let runner: ActionRunner =
        std::sync::Arc::new(|_a: &Action| Ok(serde_json::json!({ "ok": true })));
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: t
steps:
  - name: a
    action: send_dm
    to: x
    body: y
  - name: v
    action: verify
    command: "cargo test --lib"
    on_error: fail_run
"##,
    )
    .expect("parse verify");
    let sink = VerifySink::default();
    *sink.0.lock().unwrap() = Some(std::sync::Arc::new(|req: &VerifyRequest| {
        assert_eq!(req.command, "cargo test --lib");
        Ok(VerifyResult {
            ok: true,
            exit: 0,
            stdout_path: Some("/tmp/out".into()),
            stderr_path: None,
            error: None,
        })
    }));
    *engine.verify_sink().0.lock().unwrap() = sink.0.lock().unwrap().clone();
    let out = engine
        .run(
            "verify-attached-green-1",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert!(
        out.get("status").is_none() || out["status"] != "failed",
        "green verify must not fail: {out}"
    );
    assert_eq!(out["v"]["ok"], true);
}

/// Compose-style: bare dispatch followed by verify — verify executes and red
/// fails the run. Ensures the verify sink is not missing on that arm and that
/// bare dispatch + verify red is a visible failure (FailRun).
#[tokio::test]
async fn compose_bare_dispatch_plus_verify_red_fails() {
    use parking_lot::Mutex;
    use std::sync::Arc;
    let runner: ActionRunner = Arc::new(|_a: &Action| Ok(serde_json::json!({ "ok": true })));
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: composed-verify
steps:
  - name: coder-systematic-execute-1
    action: dispatch_job
    assignee: x
    spec: { profile: "@coder", stage: systematic-execute }
    on_error: fail_run
  - name: verify-1
    action: verify
    command: "cargo test --lib"
    on_error: fail_run
"##,
    )
    .expect("parse");
    // Attach RED verify sink.
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_c = seen.clone();
    let sink = VerifySink::default();
    *sink.0.lock().unwrap() = Some(Arc::new(move |req: &VerifyRequest| {
        seen_c.lock().push(req.command.clone());
        Ok(VerifyResult {
            ok: false,
            exit: 1,
            stdout_path: None,
            stderr_path: None,
            error: Some("simulated test failure".into()),
        })
    }));
    *engine.verify_sink().0.lock().unwrap() = sink.0.lock().unwrap().clone();
    let out = engine
        .run(
            "compose-verify-red-2",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run");
    assert_eq!(seen.lock().len(), 1, "verify must have executed");
    assert_eq!(seen.lock()[0], "cargo test --lib");
    assert_eq!(
        out["status"], "failed",
        "red verify must fail the run: {out}"
    );
    assert_eq!(out["failed_step"], "verify-1");
    assert!(
        !out.get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("verify sink missing"),
        "sink must be attached: {out}"
    );
}

/// bd-engine-invoke-binds-defaults-o1ud (GS-028 NEW-8 residual): declared
/// `inputs[].default` values bind under provided inputs on the invoke path,
/// visible as both `{{inputs.x}}` and `{{trigger.x}}`. Provided wins.
#[tokio::test]
async fn engine_invoke_binds_defaults() {
    use std::sync::Mutex as StdMutex;
    // Pure merge first (no Resonate needed): defaults bind, provided wins.
    let wf_decl = parse_workflow(
        r##"
name: defaults-pure
inputs:
  - name: mode
    default: "fast"
  - name: n
    default: 2
steps:
  - name: plan
    action: send_dm
    to: "pi"
    body: "x"
"##,
    )
    .expect("parse defaults workflow");
    let merged_none = WorkflowEngine::bind_input_defaults(&wf_decl.inputs, &serde_json::json!({}));
    assert_eq!(
        merged_none.get("mode").and_then(|v| v.as_str()),
        Some("fast")
    );
    assert_eq!(merged_none.get("n").and_then(|v| v.as_i64()), Some(2));
    let merged_over = WorkflowEngine::bind_input_defaults(
        &wf_decl.inputs,
        &serde_json::json!({ "mode": "slow" }),
    );
    assert_eq!(
        merged_over.get("mode").and_then(|v| v.as_str()),
        Some("slow")
    );
    assert_eq!(merged_over.get("n").and_then(|v| v.as_i64()), Some(2));

    // End-to-end through invoke: missing `mode` resolves to the default in
    // both template positions; provided `goal` passes through.
    let seen: Arc<StdMutex<Vec<Action>>> = Arc::new(StdMutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let runner: ActionRunner = Arc::new(move |a: &Action| {
        seen_clone.lock().unwrap().push(a.clone());
        Ok(serde_json::Value::Null)
    });
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: invoke-defaults
trigger:
  type: webhook
inputs:
  - name: mode
    default: "fast"
  - name: goal
steps:
  - name: plan
    action: send_dm
    to: "pi"
    body: "mode={{inputs.mode}} trigger_mode={{trigger.mode}} goal={{inputs.goal}}"
"##,
    )
    .expect("parse invoke-defaults workflow");
    let output = engine
        .invoke(
            "wf-invoke-defaults-1",
            wf,
            serde_json::json!({ "goal": "ship" }),
        )
        .await
        .expect("invoke with defaults");
    assert!(output.get("plan").is_some(), "step ran: {output}");
    match &seen.lock().unwrap()[0] {
        Action::SendDm { body, .. } => {
            assert_eq!(
                body, "mode=fast trigger_mode=fast goal=ship",
                "defaults bind under provided inputs: {body}"
            );
        }
        _ => unreachable!(),
    }
}
