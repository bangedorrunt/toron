// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Durable control-flow functions (F21.ENGINE-SPLIT move from the former
//! single-file engine): `run_workflow` + step dispatch, retry/branch/foreach/
//! parallel/call_workflow/spawn, the DAG scheduler, the parallel/graph durable
//! children, and the workflow-emission helpers (stamp + kind-46012 terminal).
//! All items are re-exported from the parent module.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use resonate_sdk::prelude::Context;

use crate::schema::{Action, Graph, Step, StepOnError};

use super::template::render_action;
use super::{
    ActionRunner, ApprovalSink, Decision, ProfileResolver, RunInput, SpawnSink, WorkflowResolver,
};

/// The durable workflow function: executes steps, durable-sleeps on `delay`,
/// and parks on `request_approval` until the promise resolves (WF-08 fix).
///
/// Action failures are captured per-step (`{"ok":false,"error":…}`) rather
/// than aborting the run — the run completes so durable state stays
/// consistent; the error is observable in the kind-46012 output. (ADR-0005
/// Spec 0 adds an opt-in `on_error: fail_run` in a later phase.)
#[resonate_sdk::function]
pub async fn run_workflow(
    ctx: &Context,
    input: RunInput,
) -> resonate_sdk::Result<serde_json::Value> {
    let runner = ctx.get_dependency::<ActionRunner>();
    let workflow = &input.workflow;

    // Graph dispatch (ADR-0005 Spec 4b): when `graph:` is present the DAG
    // scheduler runs (independent nodes concurrently), NOT the linear steps.
    if let Some(graph) = &workflow.graph {
        return run_graph(ctx, &input.trigger, graph).await;
    }

    let mut step_outputs: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    if let Some(failed) = run_steps(
        ctx,
        &runner,
        &input.trigger,
        &workflow.steps,
        &mut step_outputs,
    )
    .await?
    {
        return Ok(failed);
    }
    Ok(serde_json::Value::Object(step_outputs))
}

/// Execute an ordered list of steps, accumulating outputs into `step_outputs`
/// (under each step's `name`). Recursed into by control-flow actions
/// (ADR-0005 Spec 4): `retry`/`branch`/`foreach`/`parallel`/`call_workflow`.
///
/// Returns `Ok(None)` when the run completed normally; `Ok(Some(failed))`
/// when a step with `on_error: fail_run` failed — the failed-outcome value is
/// the run's terminal result (propagated to the caller, never stored under a
/// step name).
pub(crate) async fn run_steps(
    ctx: &Context,
    runner: &ActionRunner,
    trigger: &serde_json::Value,
    steps: &[Step],
    step_outputs: &mut serde_json::Map<String, serde_json::Value>,
) -> resonate_sdk::prelude::Result<Option<serde_json::Value>> {
    for step in steps {
        let value = run_step(ctx, runner, trigger, step, step_outputs).await?;
        // fail_run short-circuit: the failed outcome IS the run's result.
        if value.get("status").and_then(serde_json::Value::as_str) == Some("failed") {
            return Ok(Some(value));
        }
        if let Some(name) = &step.name {
            step_outputs.insert(name.clone(), value);
        }
    }
    Ok(None)
}

/// Execute ONE step (the linear body extracted from `run_workflow`, plus the
/// P5.3 control-flow actions). Returns the step's output value.
///
/// Boxed because control-flow actions recurse (`run_step` → `run_retry` →
/// `run_step`, …) — async recursion requires a heap-allocated future.
pub(crate) fn run_step<'a>(
    ctx: &'a Context,
    runner: &'a ActionRunner,
    trigger: &'a serde_json::Value,
    step: &'a Step,
    step_outputs: &'a mut serde_json::Map<String, serde_json::Value>,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<Output = resonate_sdk::prelude::Result<serde_json::Value>>
            + Send
            + 'a,
    >,
> {
    Box::pin(async move {
        let value = match &step.action {
            Action::Delay { secs } => {
                // Durable sleep — UNBOUNDED (the Buzz 270s cap is gone;
                // hours/days/weeks for SLA timers, cron-like cadence).
                ctx.sleep(Duration::from_secs(*secs)).await?;
                serde_json::Value::Null
            }
            Action::RequestApproval { message } => {
                // Create the promise; its id is the approval key. The 0.6 SDK's
                // PromiseTask is created eagerly (.create()); the handle's id()
                // confirms server-side creation.
                let pending = ctx
                    .promise::<Decision>()
                    .data(&serde_json::json!({ "message": message }))?
                    .create()?;
                let promise_id = pending.id().await?;
                // The binding: the embedding publishes kind-46011 with this
                // promise_id; the operator's approval/rejection resolves it.
                step_outputs.insert(
                    "approval".into(),
                    serde_json::json!({ "promise_id": promise_id, "message": message }),
                );
                // PARK here: this await survives a crash (replay resumes at
                // this point when the promise resolves).
                step_outputs.insert(
                    "approval".into(),
                    serde_json::json!({
                        "promise_id": promise_id,
                        "message": message,
                    }),
                );
                // Side-channel the promise_id (embedding publishes kind-46011).
                ctx.get_dependency::<ApprovalSink>()
                    .lock()
                    .await
                    .push(promise_id.clone());
                // Resolve OR rejection both complete the run — a rejected
                // approval is a DECISION, never a run failure (the WF-08 fix:
                // Buzz marks it failed; toron records the rejection).
                //
                // 0.6 semantics: awaiting a PENDING promise returns
                // `Err(Error::Suspended)`, which must PROPAGATE so the runtime
                // parks the run. A REJECTED promise (operator `promises.reject`)
                // returns `Err(Error::Application)` — that is the decision.
                let decision: Decision = match pending.await {
                    Ok(d) => d,
                    Err(resonate_sdk::error::Error::Suspended) => {
                        return Err(resonate_sdk::error::Error::Suspended);
                    }
                    Err(e) => Decision::rejected(format!("rejected: {e}")),
                };
                let decision = serde_json::to_value(&decision)?;
                step_outputs.insert("decision".into(), decision.clone());
                decision
            }
            Action::SendMessage { .. }
            | Action::SendDm { .. }
            | Action::React { .. }
            | Action::DispatchJob { .. }
            | Action::CallWebhook { .. } => {
                // Consistency rule: actions go through the shared Mailbox
                // surface (the injected action runner). Templates are
                // substituted first so the runner sees rendered values. A
                // render failure (unserializable action) is a per-step error,
                // not a run failure.
                match render_action(&step.action, trigger, step_outputs) {
                    Ok(action) => match (runner)(&action) {
                        Ok(value) => value,
                        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                    },
                    Err(e) => serde_json::json!({ "ok": false, "error": format!("render: {e}") }),
                }
            }
            Action::Retry {
                max_attempts,
                backoff_secs,
                step: inner,
            } => run_retry(ctx, runner, trigger, inner, *max_attempts, *backoff_secs).await?,
            Action::Branch {
                if_guard,
                then,
                else_steps,
            } => run_branch(ctx, runner, trigger, if_guard, then, else_steps).await?,
            Action::Foreach { over, each } => {
                run_foreach(ctx, runner, trigger, over, each, step_outputs).await?
            }
            Action::Parallel { over, each } => {
                run_parallel(ctx, runner, trigger, over, each).await?
            }
            Action::CallWorkflow { workflow, inputs } => {
                run_call_workflow(ctx, runner, workflow, inputs, step_outputs).await?
            }
            Action::Spawn {
                profile,
                workflow,
                await_,
                no_claim_timeout,
            } => {
                run_spawn(
                    ctx,
                    profile,
                    workflow.as_deref(),
                    *await_,
                    *no_claim_timeout,
                )
                .await?
            }
        };

        // P0.2 (ADR-0005 Spec 0): `on_error: fail_run` converts a step failure
        // into a RUN failure — a deterministic durable RETURN, never a panic.
        if step.on_error == StepOnError::FailRun
            && value.get("ok").and_then(serde_json::Value::as_bool) == Some(false)
        {
            let error = value["error"].as_str().unwrap_or("step failed").to_string();
            return Ok(serde_json::json!({
                "status": "failed",
                "error": error,
                "failed_step": step.name.clone().unwrap_or_else(|| "<unnamed>".into()),
            }));
        }
        Ok(value)
    })
}

/// Run a `retry` action: attempt the inner step up to `max_attempts` times,
/// waiting `backoff_secs` between failures. Returns the inner step's output;
/// if all attempts fail, returns `{ok:false,error}` (the on_error policy at
/// the OUTER step decides run fate).
pub(crate) async fn run_retry(
    ctx: &Context,
    runner: &ActionRunner,
    trigger: &serde_json::Value,
    inner: &Step,
    max_attempts: u32,
    backoff_secs: u64,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let mut last_err: Option<String> = None;
    for attempt in 1..=max_attempts.max(1) {
        let mut outputs = serde_json::Map::new();
        let value = run_step(ctx, runner, trigger, inner, &mut outputs).await?;
        let failed = value.get("ok").and_then(serde_json::Value::as_bool) == Some(false);
        if !failed {
            return Ok(value);
        }
        last_err = value
            .get("error")
            .and_then(|e| e.as_str())
            .map(String::from);
        if attempt < max_attempts.max(1) && backoff_secs > 0 {
            ctx.sleep(Duration::from_secs(backoff_secs)).await?;
        }
    }
    Ok(serde_json::json!({
        "ok": false,
        "error": last_err.unwrap_or_else(|| "retry exhausted".into()),
        "retried": true,
    }))
}

/// Run a `branch` action: evaluate the guard; run the `then` or `else` step
/// list. Returns `{branch: "then"|"else", outputs: {...}}`.
pub(crate) async fn run_branch(
    ctx: &Context,
    runner: &ActionRunner,
    trigger: &serde_json::Value,
    guard: &str,
    then: &[Step],
    else_steps: &[Step],
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    use crate::guard::evaluate_guard_default;
    // Build the guard variable map from the trigger payload: `{{trigger.x}}`
    // (lint substitutes these to valid identifiers before eval).
    let mut vars = std::collections::HashMap::new();
    if let Some(obj) = trigger.as_object() {
        for (k, v) in obj {
            let name = format!("trigger_{}", k);
            let text = match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            vars.insert(name, evalexpr::Value::from(text));
        }
    }
    let outcome = evaluate_guard_default(Some(guard), &vars).await;
    let take_then = matches!(outcome, crate::guard::GuardOutcome::Bool(true));
    let branch_steps = if take_then { then } else { else_steps };
    let mut outputs = serde_json::Map::new();
    if let Some(failed) = run_steps(ctx, runner, trigger, branch_steps, &mut outputs).await? {
        return Ok(failed);
    }
    Ok(serde_json::json!({
        "branch": if take_then { "then" } else { "else" },
        "outputs": serde_json::Value::Object(outputs),
    }))
}

/// Run a `foreach` action: resolve `over` to a list; run `each` once per
/// item; collect the sub-run outputs into a list.
pub(crate) async fn run_foreach(
    ctx: &Context,
    runner: &ActionRunner,
    trigger: &serde_json::Value,
    over: &str,
    each: &Step,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let list = resolve_template_ref(over, trigger, step_outputs)
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default();
    let mut collected = Vec::new();
    for item in &list {
        // Each iteration sees the current item as `{{item}}` (ADR-0007 F4):
        // inject it into the iteration's trigger copy so the template
        // renderer can resolve it.
        let mut iter_trigger = serde_json::Value::Object(serde_json::Map::new());
        if let Some(obj) = trigger.as_object() {
            iter_trigger = serde_json::Value::Object(obj.clone());
        }
        iter_trigger["item"] = item.clone();
        let mut item_outputs = serde_json::Map::new();
        let value = run_step(ctx, runner, &iter_trigger, each, &mut item_outputs).await?;
        collected.push(value);
    }
    Ok(serde_json::Value::Array(collected))
}

/// Run a `parallel` action: fan `each` out across a crew role (`over:
/// "@crew.r"`) or N elastic copies (`over: "<N>"`). For elastic N, spawns N
/// concurrent sub-runs of `each`; collected into a list. Crew-role fan-out
/// (the orchestrator steers the actual spawned agents) is resolved by the
/// embedder via the `ProfileResolver` DI seam — here N elastic copies run.
pub(crate) async fn run_parallel(
    ctx: &Context,
    _runner: &ActionRunner,
    trigger: &serde_json::Value,
    over: &str,
    each: &Step,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // Elastic N: `over: "4"` → 4 concurrent copies. Crew role (@crew.r) is
    // resolved by the orchestrator before invoke; the engine sees a concrete N.
    let n: usize = over.trim().parse().unwrap_or(1);
    let mut futures = Vec::new();
    for _ in 0..n.max(1) {
        let each = each.clone();
        let trigger = trigger.clone();
        let future = ctx
            .run(
                run_parallel_copy,
                ParallelCopyInput {
                    step: each,
                    trigger,
                },
            )
            .spawn()?;
        futures.push(future);
    }
    let mut collected = Vec::new();
    for future in futures {
        collected.push(future.await?);
    }
    Ok(serde_json::Value::Array(collected))
}

/// The input for one parallel copy (a durable child).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelCopyInput {
    pub step: Step,
    pub trigger: serde_json::Value,
}

/// A durable child running one `parallel` copy of a step. The runner is
/// re-fetched from the runtime dependency (the consistency rule).
#[resonate_sdk::function]
pub async fn run_parallel_copy(
    ctx: &Context,
    input: ParallelCopyInput,
) -> resonate_sdk::Result<serde_json::Value> {
    let runner = ctx.get_dependency::<ActionRunner>();
    let mut outputs = serde_json::Map::new();
    let value = run_step(ctx, &runner, &input.trigger, &input.step, &mut outputs).await?;
    Ok(value)
}

/// Run a `call_workflow` action: invoke another workflow as a sub-run
/// (idempotent on a deterministic sub-run id derived from the parent run +
/// step). Outputs flow into `steps.<name>` via the returned value.
pub(crate) async fn run_call_workflow(
    ctx: &Context,
    _runner: &ActionRunner,
    workflow: &str,
    inputs: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // ADR-0007 R4.1: nested durable run via WorkflowResolver DI + ctx.run.
    let resolver = ctx.get_dependency::<WorkflowResolver>();
    let Some(child_wf) = (resolver)(workflow) else {
        return Ok(serde_json::json!({
            "ok": false,
            "error": format!("unknown workflow: {workflow}"),
        }));
    };
    // Child trigger = rendered inputs object (templates already expanded in
    // the Action by the parent step path when present); fall back to raw
    // inputs + expose parent step_outputs under `parent_steps` for templates.
    let mut trigger = inputs.clone();
    if let Some(obj) = trigger.as_object_mut() {
        obj.insert(
            "parent_steps".into(),
            serde_json::Value::Object(step_outputs.clone()),
        );
    }
    let input = RunInput {
        workflow: child_wf,
        trigger,
    };
    let future = ctx.run(run_workflow, input).spawn()?;
    let out = future.await?;
    Ok(out)
}

/// Run a `spawn` action (ADR-0005 Spec 4a).
///
/// 1. Resolve `profile` via the injected `ProfileResolver` DI seam.
/// 2. Create a promise; its id is the job's promise↔job binding key. The
///    embedding emits kind-43001 carrying the promise_id in its `spec`.
/// 3. `await: true` → PARK on the promise until a terminal job event
///    (43004/43006) resolves it — a FAILURE, not a decision. `no_claim_timeout`
///    rejects an unclaimed job with `timeout` → the step fails → Spec 0
///    propagates.
///
/// 0.6 semantics: awaiting a pending promise returns `Err(Error::Suspended)`,
/// which must PROPAGATE so the runtime parks the run.
pub(crate) async fn run_spawn(
    ctx: &Context,
    profile: &str,
    workflow: Option<&str>,
    await_: bool,
    no_claim_timeout: u64,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    // 1. Resolve the profile (DI seam).
    let resolver = ctx.get_dependency::<ProfileResolver>();
    let resolved = match (resolver)(profile) {
        Some(v) => v,
        None => {
            return Ok(serde_json::json!({
                "ok": false,
                "error": format!("spawn: unknown profile {profile:?} (ProfileResolver returned None)"),
            }));
        }
    };

    // 2. Create the promise — the job's binding key.
    let spec = serde_json::json!({
        "profile": profile,
        "workflow": workflow,
        "resolved": resolved,
        // bd-omwg: the embedding's drainer must know whether the run parks on
        // this job (resolve on completion) or fired it detached (fire-and-
        // forget, never resolve).
        "await": await_,
    });
    let task = ctx.promise::<serde_json::Value>().data(&spec)?;
    let task = if await_ && no_claim_timeout > 0 {
        task.timeout(Duration::from_secs(no_claim_timeout))
    } else {
        task
    };
    let pending = task.create()?;
    let promise_id = pending.id().await?;

    // Side-channel the promise_id (embedding emits kind-43001 with it in spec).
    ctx.get_dependency::<SpawnSink>()
        .lock()
        .await
        .push(promise_id.clone());

    // 3. No await → fire the job and return immediately (no park).
    if !await_ {
        return Ok(serde_json::json!({
            "ok": true,
            "promise_id": promise_id,
            "spawned": true,
        }));
    }

    match pending.await {
        Ok(value) => Ok(serde_json::json!({
            "ok": true,
            "promise_id": promise_id,
            "result": value,
        })),
        Err(resonate_sdk::error::Error::Suspended) => Err(resonate_sdk::error::Error::Suspended),
        Err(e) => {
            let msg = format!("spawn-await {promise_id}: {e}");
            Ok(serde_json::json!({ "ok": false, "error": msg }))
        }
    }
}

/// Resolve a template reference (`{{steps.x}}` or a literal path) to a JSON
/// value from the current outputs/trigger.
pub(crate) fn resolve_template_ref(
    token: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let trimmed = token.trim();
    if let Some(inner) = trimmed
        .strip_prefix("{{")
        .and_then(|s| s.strip_suffix("}}"))
    {
        return super::template::resolve_template(inner, trigger, step_outputs)
            .and_then(|s| serde_json::from_str(&s).ok())
            .or_else(|| {
                super::template::resolve_template(inner, trigger, step_outputs)
                    .map(serde_json::Value::String)
            });
    }
    serde_json::from_str(trimmed).ok()
}

/// The DAG scheduler (ADR-0005 Spec 4b / P5.2). Runs a `graph:` workflow:
///
/// 1. Schedule nodes in waves — a wave is the set of nodes whose `needs` are
///    all satisfied (roots first).
/// 2. Within a wave, spawn every node as a concurrent durable child
///    (`ctx.run(run_graph_node).spawn()`) — true concurrency inside one
///    deterministic-replay function (the P5.1 spike proved this primitive).
/// 3. Await the wave's futures, collect outputs under `steps.<name>`.
/// 4. Repeat until all nodes done.
///
/// The spike's bit-identical-replay guarantee carries over: child spawn order
/// is the deterministic per-context seq, so a crash mid-run replays to the
/// same outputs, promise ids, and child-spawn order.
pub(crate) async fn run_graph(
    ctx: &Context,
    trigger: &serde_json::Value,
    graph: &Graph,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    use crate::schema::GraphNode;
    use std::collections::BTreeSet;

    let nodes = &graph.nodes;
    let mut outputs: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();
    let mut done: BTreeSet<String> = BTreeSet::new();

    loop {
        // Compute the next ready wave: not done, all needs done.
        let mut ready: Vec<(&String, &GraphNode)> = Vec::new();
        for (name, node) in nodes {
            if done.contains(name) {
                continue;
            }
            let needs_satisfied = node.needs.iter().all(|need| done.contains(need));
            if needs_satisfied {
                ready.push((name, node));
            }
        }
        if ready.is_empty() {
            break; // nothing ready: either all done or a cycle (lint prevents).
        }

        // Spawn the whole wave concurrently as durable children.
        let mut futures = Vec::new();
        for (name, node) in &ready {
            let rendered = match render_action(&node.action, trigger, &outputs) {
                Ok(a) => a,
                Err(e) => {
                    // A render failure is a per-node error (continue unless
                    // fail_run).
                    let err = serde_json::json!({ "ok": false, "error": format!("render: {e}") });
                    if node.on_error == StepOnError::FailRun {
                        return Ok(serde_json::json!({
                            "status": "failed",
                            "error": format!("render: {e}"),
                            "failed_step": name,
                        }));
                    }
                    outputs.insert((*name).clone(), err);
                    done.insert((*name).clone());
                    continue;
                }
            };
            let future = ctx
                .run(
                    run_graph_node,
                    GraphNodeInput {
                        action: rendered,
                        trigger: trigger.clone(),
                    },
                )
                .spawn()?;
            futures.push(((*name).clone(), future));
        }

        // Await the wave's children (they ran concurrently; order of await is
        // deterministic).
        for (name, future) in futures {
            let value = future.await?;
            // on_error: fail_run — a child failure must fail the run.
            let node = nodes.get(&name).expect("node in map");
            if let Some(err) = value.get("error").and_then(|v| v.as_str())
                && node.on_error == StepOnError::FailRun
            {
                return Ok(serde_json::json!({
                    "status": "failed",
                    "error": err,
                    "failed_step": name,
                }));
            }
            outputs.insert(name.clone(), value);
            done.insert(name);
        }
    }

    Ok(serde_json::Value::Object(outputs))
}

/// The input for one graph node execution (the durable child's input).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNodeInput {
    /// The (template-rendered) action to execute.
    pub action: Action,
    /// The trigger payload (bd-m7ac): control-flow nodes (parallel/foreach/
    /// branch) need it to render their inner steps — the raw runner path did
    /// not. Serialized with the action so the child is self-contained.
    pub trigger: serde_json::Value,
}

/// A durable child executing ONE graph node's action. Spawned concurrently by
/// the DAG scheduler via `ctx.run(...).spawn()`. Runs through the shared
/// `ActionRunner` (the Mailbox surface — the consistency rule). Failures are
/// returned as `{ok:false,error}` values (the parent applies on_error).
#[resonate_sdk::function]
pub async fn run_graph_node(
    ctx: &Context,
    input: GraphNodeInput,
) -> resonate_sdk::Result<serde_json::Value> {
    // bd-m7ac: graph nodes must route CONTROL-FLOW actions through the same
    // expansion as linear steps. A graph `parallel` node (the weekly-review
    // scout fan-out) needs `run_parallel` to spawn its copies — dispatching
    // the raw action to the runner would record `{action:"parallel"}` and
    // never fan out. Leaf actions keep the direct runner path (rendered
    // before the node ran, so no re-render).
    let runner = ctx.get_dependency::<ActionRunner>();
    use crate::schema::Step;
    let is_control_flow = matches!(
        input.action,
        Action::Parallel { .. }
            | Action::Foreach { .. }
            | Action::Branch { .. }
            | Action::Retry { .. }
            | Action::CallWorkflow { .. }
    );
    if is_control_flow {
        let mut step_outputs = serde_json::Map::new();
        let step = Step {
            name: None,
            guard: None,
            on_error: crate::schema::StepOnError::Continue,
            action: input.action,
        };
        return run_step(ctx, &runner, &input.trigger, &step, &mut step_outputs).await;
    }
    match (runner)(&input.action) {
        Ok(value) => Ok(value),
        Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
    }
}

/// The `toron:workflow-emitted` stamp (ADR-0004 Ship 2 / bd-pkc part 3).
///
/// The ActionRunner's embedding applies this single-`t` tag to every event a
/// workflow action emits, so the trigger-ingress filter
/// ([`crate::trigger_filter::should_evaluate_trigger_event`]) rejects
/// workflow-emitted events by tag — loop-prevention defense-in-depth
/// (workflow emits never re-trigger the same workflow).
///
/// The stamp is a pure builder: given a tags vector, returns a new vector
/// with the marker `t`-tag appended (idempotent — never double-stamps).
pub fn stamp_workflow_emitted(tags: Vec<nostr_sdk::prelude::Tag>) -> Vec<nostr_sdk::prelude::Tag> {
    use crate::trigger_filter::WORKFLOW_EMITTED_TAG;
    if tags.iter().any(|t| {
        t.as_slice().first().map(String::as_str) == Some("t")
            && t.as_slice().get(1).map(String::as_str) == Some(WORKFLOW_EMITTED_TAG)
    }) {
        return tags;
    }
    let mut out = tags;
    out.push(nostr_sdk::prelude::Tag::parse(["t", WORKFLOW_EMITTED_TAG]).expect("stamp tag"));
    out
}

/// Build the kind-46012 workflow-run terminal event (ADR-0005 Spec 0, P0.3).
/// Maps the durable function's outcome Value to the wire `status` field —
/// additive content only (the kind is frozen; its content is toron-owned; old
/// readers ignore the new `status`/`run_id`/`error`/`failed_step` fields):
///
/// - outcome `{status:"failed", error, failed_step}` → 46012 `status:"failed"`.
/// - normal completion (no `status` key) → 46012 `status:"succeeded"`.
///
/// The full outcome is preserved under `output` so downstream readers (smart
/// cron's `on_failure`, `flywheel runs`) see step outputs on success and the
/// failure bundle on failure.
///
/// The event is addressable by run id (`d` = run_id), matching the
/// WORKFLOW_RUN_FIRST..=LAST registry contract.
pub fn build_workflow_run_terminal(
    keys: &nostr_sdk::prelude::Keys,
    run_id: &str,
    outcome: &serde_json::Value,
) -> anyhow::Result<nostr_sdk::prelude::Event> {
    use nostr_sdk::prelude::{Tag, Timestamp};
    use toron_core::nostr::event::build_event;
    use toron_core::nostr::kinds::{WORKFLOW_RUN_LAST, toron_kind};

    let status = match outcome.get("status").and_then(serde_json::Value::as_str) {
        Some("failed") => "failed",
        _ => "succeeded",
    };
    let mut terminal = serde_json::Map::new();
    terminal.insert("status".into(), serde_json::Value::String(status.into()));
    if let Some(err) = outcome.get("error") {
        terminal.insert("error".into(), err.clone());
    }
    if let Some(failed_step) = outcome.get("failed_step") {
        terminal.insert("failed_step".into(), failed_step.clone());
    }
    terminal.insert("run_id".into(), serde_json::Value::String(run_id.into()));
    terminal.insert("output".into(), outcome.clone());

    build_event(
        keys,
        toron_kind(WORKFLOW_RUN_LAST),
        serde_json::Value::Object(terminal).to_string(),
        vec![Tag::identifier(run_id)],
        Timestamp::now(),
    )
    .map_err(|e| anyhow::anyhow!("build kind-46012 terminal: {e}"))
}
