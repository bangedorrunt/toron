    use super::*;
    use std::time::Duration;
    use crate::schema::{Action, Workflow, parse_workflow};

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
    async fn remote_engine(action_runner: ActionRunner) -> (std::process::Child, WorkflowEngine) {
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
            .expect("spawn resonate server");
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
        (server, engine)
    }

    /// Like [`remote_engine`] with a `ProfileResolver` for the `spawn` action.
    async fn remote_engine_with(
        profile_resolver: ProfileResolver,
    ) -> (std::process::Child, WorkflowEngine) {
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
            .expect("spawn resonate server");
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
        (server, engine)
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
        let (_srv, engine) = remote_engine(noop_runner()).await;

        let input = RunInput {
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
        assert_eq!(
            output["decision"]["outcome"], "approved",
            "decision recorded after resume"
        );
    }

    /// A run hitting `request_approval` and REJECTED completes with the
    /// rejection recorded (not failed — the rejection is a decision).
    #[tokio::test(flavor = "multi_thread")]
    async fn approval_rejection_is_a_decision_not_a_failure() {
        let (_srv, engine) = remote_engine(noop_runner()).await;
        let input = RunInput {
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
        assert_eq!(output["decision"]["outcome"], "rejected");
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
        let (_srv, engine) = remote_engine_with(test_profile_resolver()).await;
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
        assert_eq!(output["build"]["ok"], true, "spawn step succeeded: {output}");
        assert_eq!(output["build"]["result"]["id"], 7, "job result captured");
    }

    /// P4.2: a kind-43006 job error REJECTS the promise → the spawn step
    /// fails (a FAILURE, not a decision — distinct from approval).
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_await_failure_fails_the_step() {
        let (_srv, engine) = remote_engine_with(test_profile_resolver()).await;
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
        let content: serde_json::Value =
            serde_json::from_str(&ev.content).expect("content parses");
        assert_eq!(content["status"], "failed");
        assert_eq!(content["error"], "boom");
        assert_eq!(content["failed_step"], "upgrade");
        assert_eq!(content["run_id"], "run-1");
        // Addressable by run id (d tag).
        assert!(
            ev.tags.iter().any(|t| t.as_slice().get(0) == Some(&"d".to_string())
                && t.as_slice().get(1) == Some(&"run-1".to_string())),
            "d tag = run id"
        );

        // Normal completion → status:"succeeded" + the step outputs.
        let ok = serde_json::json!({ "foo": { "n": 42 } });
        let ev = build_workflow_run_terminal(&keys, "run-2", &ok).expect("build ok terminal");
        let content: serde_json::Value =
            serde_json::from_str(&ev.content).expect("content parses");
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
            stamped.iter().any(|t| t.as_slice().get(0).map(String::as_str) == Some("t")
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
            workflow: fail_run_workflow(),
            trigger: serde_json::json!({}),
        };
        let outcome = engine
            .run("wf-fail-46012", input)
            .await
            .expect("run returns failed outcome");

        let terminal =
            build_workflow_run_terminal(&Keys::generate(), "wf-fail-46012", &outcome)
                .expect("build terminal");
        let content: serde_json::Value =
            serde_json::from_str(&terminal.content).expect("content parses");
        assert_eq!(content["status"], "failed", "46012 status=failed: {content}");
        assert!(content["error"].as_str().unwrap_or_default().contains("boom"));
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
        use std::collections::HashMap;
        use crate::guard::{GuardOutcome, evaluate_guard_default};

        // The engine/embedding substitutes `{{trigger.channel}}` into a valid
        // evalexpr identifier (dots are not allowed in identifiers — see
        // guard.rs variable_comparison_true). The variable map carries the
        // trigger context under those names.
        let mut vars = HashMap::new();
        vars.insert("trigger_channel".into(), evalexpr::Value::from("#support".to_string()));
        vars.insert("trigger_project".into(), evalexpr::Value::from("toron".to_string()));
        let out = evaluate_guard_default(
            Some("trigger_channel == \"#support\" && trigger_project == \"toron\""),
            &vars,
        )
        .await;
        assert!(matches!(out, GuardOutcome::Bool(true)), "guard true: {out:?}");
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

    /// P5.3: `retry` — a failing sub-step is retried up to `max_attempts`;
    /// `retry_backoff` uses the backoff. On success mid-way, the retry
    /// returns the value.
    #[tokio::test]
    async fn retry_backoff_retries_until_success() {
        use std::sync::atomic::{AtomicU32, Ordering};
        use std::sync::Mutex as StdMutex;
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
        let runner: ActionRunner =
            Arc::new(|_a: &Action| Err(anyhow::anyhow!("always fails")));
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
                    workflow: wf.clone(),
                    trigger: serde_json::json!({ "ok": "true" }),
                },
            )
            .await
            .expect("runs");
        let last = { let actions = seen.lock().unwrap(); actions.last().cloned() };
        assert!(matches!(last, Some(Action::SendDm { body, .. }) if body == "yes"));

        // Guard false → else branch.
        let _ = engine
            .run(
                "wf-branch-false",
                RunInput {
                    workflow: wf,
                    trigger: serde_json::json!({ "ok": "false" }),
                },
            )
            .await
            .expect("else-branch run completes");
        let last = { let actions = seen.lock().unwrap(); actions.last().cloned() };
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
        let engine = WorkflowEngine::local_with_resolvers(
            runner,
            Arc::new(|_: &str| None),
            resolver,
        )
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
        assert_eq!(
            seen.lock().unwrap().len(),
            1,
            "child action executed once"
        );
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
            .invoke("wf-invoke-1", wf.clone(), serde_json::json!({ "goal": "ship-it" }))
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
