// governed-by: ADR-0032
//!
//! ADR-0032 Specs 3+4 + Spec-7 control-flow RED tests (bd-o1hl).
//!
//! Each test names a degradation the bead forbids; all must FAIL before the
//! fix and PASS after. Proof target: one guard/ref evaluator everywhere,
//! runtime graph validation even when lint is bypassed, no silent coercions,
//! one retry coordinator, Spawn timeout semantics, call_workflow/invoke parity.

use std::sync::{Arc, Mutex as StdMutex};

use toron_workflow::engine::{ActionRunner, RunInput, WorkflowEngine};
use toron_workflow::schema::{Action, parse_workflow};

fn recording_runner(
    seen: Arc<StdMutex<Vec<Action>>>,
    f: impl Fn(&Action) -> serde_json::Value + Send + Sync + 'static,
) -> ActionRunner {
    let seen_c = seen.clone();
    Arc::new(move |a: &Action| {
        seen_c.lock().unwrap().push(a.clone());
        Ok(f(a))
    })
}

fn always_ok_runner(seen: Arc<StdMutex<Vec<Action>>>) -> ActionRunner {
    recording_runner(seen, |_| serde_json::json!({ "ok": true }))
}

/// foreach over a non-list must FAIL LOUD (never coerce to []).
#[tokio::test]
async fn foreach_non_list_fails_loud_not_empty() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: foreach-bad
trigger:
  type: webhook
steps:
  - name: loop
    action: foreach
    over: "{{trigger.items}}"
    each:
      action: send_dm
      to: pi
      body: "hi"
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-foreach-nonlist",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({ "items": "not-a-list" }),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["loop"]["ok"], false,
        "non-list foreach must fail, not return []: {output}"
    );
    assert!(
        output["loop"]["error"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("list"),
        "error names the list contract: {output}"
    );
}

/// parallel with an invalid count must FAIL LOUD (never coerce to 1).
#[tokio::test]
async fn parallel_invalid_count_fails_loud_not_one() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: parallel-bad
trigger:
  type: webhook
steps:
  - name: fan
    action: parallel
    over: "abc"
    each:
      action: send_dm
      to: pi
      body: "hi"
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-parallel-bad",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["fan"]["ok"], false,
        "invalid parallel count must fail, not run 1 copy: {output}"
    );
}

/// parallel with zero count must FAIL LOUD (positive bounded counts only).
#[tokio::test]
async fn parallel_zero_count_fails_loud() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: parallel-zero
trigger:
  type: webhook
steps:
  - name: fan
    action: parallel
    over: "0"
    each:
      action: send_dm
      to: pi
      body: "hi"
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-parallel-zero",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["fan"]["ok"], false,
        "zero parallel count must fail: {output}"
    );
}

/// A cyclic graph invoked WITHOUT lint (engine.run bypasses lint) must fail
/// naming the cycle — never partial success.
#[tokio::test]
async fn runtime_graph_cycle_fails_when_lint_bypassed() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    // parse_workflow does not validate the graph; engine.run must.
    let wf = parse_workflow(
        r##"
name: cyclic
trigger:
  type: webhook
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, needs: [b] }
    b: { action: send_dm, to: x, body: y, needs: [a] }
"##,
    )
    .expect("parse succeeds; lint is bypassed");
    let output = engine
        .run(
            "wf-cycle-direct",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output.get("status").and_then(|v| v.as_str()),
        Some("failed"),
        "cyclic graph must fail the run: {output}"
    );
    let err = output.get("error").and_then(|v| v.as_str()).unwrap_or("");
    assert!(
        err.to_lowercase().contains("cycle") || err.to_lowercase().contains("stall"),
        "error names cycle/stall: {output}"
    );
}

/// Unknown `needs` invoked WITHOUT lint must fail naming the missing node.
#[tokio::test]
async fn runtime_graph_unknown_needs_fails_when_lint_bypassed() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: bad-needs
trigger:
  type: webhook
graph:
  nodes:
    a: { action: send_dm, to: x, body: y, needs: [ghost] }
"##,
    )
    .expect("parse succeeds; lint is bypassed");
    let output = engine
        .run(
            "wf-needs-direct",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output.get("status").and_then(|v| v.as_str()),
        Some("failed"),
        "unknown needs must fail the run: {output}"
    );
    assert!(
        output
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .contains("ghost"),
        "error names the missing node: {output}"
    );
}

/// A linear step guard evaluating to false must SKIP explicitly — the guarded
/// action must NOT execute.
#[tokio::test]
async fn linear_step_guard_false_skips_without_running() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen.clone())).expect("engine");
    let wf = parse_workflow(
        r##"
name: linear-guard
trigger:
  type: webhook
steps:
  - name: gated
    guard: "1 == 2"
    action: send_dm
    to: pi
    body: "must not send"
  - name: after
    action: send_dm
    to: pi
    body: "runs"
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-linear-guard",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        seen.lock().unwrap().len(),
        1,
        "guarded action must not execute; only `after` runs"
    );
    assert_eq!(
        output["gated"]["skipped"], true,
        "false guard is an explicit skip: {output}"
    );
    assert!(output.get("after").is_some(), "run continues: {output}");
}

/// A branch guard that ERRORS (unknown variable) must FAIL the step — never
/// silently take else.
#[tokio::test]
async fn branch_guard_error_fails_not_else() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: branch-err
trigger:
  type: webhook
steps:
  - name: b
    action: branch
    if: "no_such_var_xyz == 1"
    then:
      - action: send_dm
        to: pi
        body: "then"
    else:
      - action: send_dm
        to: pi
        body: "else"
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-branch-err",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["b"]["ok"], false,
        "guard error must fail the step, not take else: {output}"
    );
    assert!(
        output["b"]["error"]
            .as_str()
            .unwrap_or_default()
            .to_lowercase()
            .contains("guard"),
        "error names the guard: {output}"
    );
}

/// A graph-node guard evaluating to a non-bool must FAIL the node — never a
/// silent Null skip.
#[tokio::test]
async fn graph_node_guard_nonbool_fails_not_silent_skip() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: node-guard
trigger:
  type: webhook
graph:
  nodes:
    n: { action: send_dm, to: x, body: y, guard: "1" }
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-node-guard",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["n"]["ok"], false,
        "non-bool node guard must fail the node: {output}"
    );
}

/// Nested retry wrappers must NOT multiply attempts: one coordinator, the
/// outermost policy wins (2x2 nests run 2 attempts, not 4).
#[tokio::test]
async fn nested_retry_does_not_multiply_attempts() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_c = calls.clone();
    let runner: ActionRunner = Arc::new(move |_: &Action| {
        calls_c.fetch_add(1, Ordering::SeqCst);
        Ok(serde_json::json!({ "ok": false, "error": "boom" }))
    });
    let engine = WorkflowEngine::local(runner).expect("engine");
    let wf = parse_workflow(
        r##"
name: nested-retry
trigger:
  type: webhook
steps:
  - name: go
    action: retry
    max_attempts: 2
    step:
      action: retry
      max_attempts: 2
      step:
        action: send_dm
        to: x
        body: y
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-nested-retry",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["go"]["ok"], false,
        "exhausted retry stays failed: {output}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "one coordinator: outer policy of 2, not 2x2=4"
    );
}

/// call_workflow must bind the child's declared defaults exactly like
/// invoke does — provided values win, missing keys fall back to defaults.
#[tokio::test]
async fn call_workflow_binds_child_defaults_like_invoke() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let runner = recording_runner(seen.clone(), |_| serde_json::json!({ "ran": true }));
    let child = parse_workflow(
        r##"
name: kid
trigger:
  type: webhook
inputs:
  - name: mode
    default: "fast"
steps:
  - name: do
    action: send_dm
    to: pi
    body: "mode={{inputs.mode}}"
"##,
    )
    .expect("child");
    let resolver: toron_workflow::engine::WorkflowResolver = {
        let child = child.clone();
        Arc::new(move |name: &str| {
            if name == "kid" {
                Some(child.clone())
            } else {
                None
            }
        })
    };
    let engine = toron_workflow::engine::WorkflowEngine::local_with_resolvers(
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
    workflow: kid
    inputs: {}
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-call-defaults",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run completes");
    assert!(
        output["sub"]["do"].is_object() || output["sub"].is_object(),
        "child output returned: {output}"
    );
    let actions = seen.lock().unwrap();
    let body = actions.iter().find_map(|a| match a {
        Action::SendDm { body, .. } => Some(body.clone()),
        _ => None,
    });
    assert_eq!(
        body.as_deref(),
        Some("mode=fast"),
        "child default binds under call_workflow exactly like invoke: {body:?}"
    );
}

/// Unresolved template refs must FAIL naming workflow/node/field/ref — never
/// render a verbatim placeholder into the runner payload.
#[tokio::test]
async fn unresolved_template_ref_fails_loud() {
    let seen = Arc::new(StdMutex::new(Vec::new()));
    let engine = WorkflowEngine::local(always_ok_runner(seen)).expect("engine");
    let wf = parse_workflow(
        r##"
name: bad-ref
trigger:
  type: webhook
steps:
  - name: m
    action: send_dm
    to: pi
    body: "{{steps.ghost.field}}"
"##,
    )
    .expect("parse");
    let output = engine
        .run(
            "wf-bad-ref",
            RunInput {
                run_id: None,
                workflow: wf,
                trigger: serde_json::json!({}),
            },
        )
        .await
        .expect("run returns");
    assert_eq!(
        output["m"]["ok"], false,
        "unresolved ref must fail the step: {output}"
    );
    let err = output["m"]["error"].as_str().unwrap_or("").to_string();
    assert!(
        err.contains("steps.ghost.field"),
        "error names the reference path: {output}"
    );
}

/// The runtime `FailureKind` vocabulary stays frozen at exactly eight
/// spellings (ADR-0026 closed set, ADR-0032 §2.2): contract errors use
/// `ActionErrorCode`, never new kinds.
#[test]
fn failure_kind_vocabulary_frozen() {
    use toron_workflow::engine::FailureKind;
    assert_eq!(
        FailureKind::ALL,
        [
            "auth",
            "rate_limit",
            "timeout",
            "blocked",
            "provider",
            "cancelled",
            "unknown",
            "billing"
        ],
        "frozen eight"
    );
    for kind in ["rate_limit", "timeout"] {
        assert!(
            kind.parse::<FailureKind>()
                .is_ok_and(|k| k.is_retry_candidate()),
            "{kind} is a retry candidate"
        );
    }
    for kind in [
        "auth",
        "billing",
        "provider",
        "blocked",
        "cancelled",
        "unknown",
    ] {
        assert!(
            kind.parse::<FailureKind>()
                .is_ok_and(|k| !k.is_retry_candidate()),
            "{kind} never blind-retries"
        );
    }
    assert!("bogus".parse::<FailureKind>().is_err(), "no new kinds");
}

/// Spawn without `no_claim_timeout` deserializes to 600s; explicit 0 is the
/// only no-timeout spelling (characterization of the schema bar).
#[test]
fn spawn_timeout_default_600_explicit_zero_only() {
    let wf = parse_workflow(
        r##"
name: spawn-t
trigger:
  type: webhook
steps:
  - name: a
    action: spawn
    profile: "@housekeeper"
    await: true
  - name: b
    action: spawn
    profile: "@housekeeper"
    await: true
    no_claim_timeout: 0
"##,
    )
    .expect("parse");
    let timeouts: Vec<u64> = wf
        .steps
        .iter()
        .map(|s| match &s.action {
            Action::Spawn {
                no_claim_timeout, ..
            } => *no_claim_timeout,
            other => panic!("expected spawn, got {other:?}"),
        })
        .collect();
    assert_eq!(
        timeouts,
        vec![600, 0],
        "absent → 600, explicit 0 → 0 (only no-timeout spelling)"
    );
}
