// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Resonate-backed workflow engine (bd-oge) — the Buzz WF-08 fix.
//!
//! The decisive property: a run that hits `request_approval` SUSPENDS, survives
//! a toron process crash, and RESUMES when the approval event arrives — NOT
//! marked failed (Buzz marks it failed; that is the bug toron fixes).
//!
//! Each run is a Resonate durable function. Durable state (execution + promise
//! records) lives in the Resonate runtime (`Resonate::local()` in-process, or a
//! real server via `ResonateConfig::url` for cross-process crash survival).
//!
//! **Approval binding (promise_id ↔ kind-46011):**
//! 1. The run hits `request_approval` → creates a Resonate promise, publishes
//!    kind-46011 `approval-pending` carrying the promise_id.
//! 2. The operator signs kind-46011 approval/rejection (echoing the
//!    promise_id) — the toron bus subscriber resolves it via
//!    `resonate.promises.resolve/reject`.
//! 3. The parked durable function unblocks → continues → kind-46012.
//!
//! **Consistency rule:** workflow ACTIONS call the same `Mailbox` methods the
//! MCP/CLI tools call — never raw bus writes. The action runner is injected as
//! a Resonate dependency; the workflow function reads it via `ctx.get_dependency`.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use resonate_sdk::prelude::{Context, Resonate, ResonateConfig};
use resonate_sdk::types::Value;

use crate::schema::{Action, Graph, Step, StepOnError, Workflow};

/// An approval decision (the promise payload).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    /// `approved` or `rejected`.
    pub outcome: String,
    /// Optional note from the approver.
    #[serde(default)]
    pub note: String,
}

impl Decision {
    /// Approved decision.
    pub fn approved(note: impl Into<String>) -> Self {
        Self {
            outcome: "approved".into(),
            note: note.into(),
        }
    }

    /// Rejected decision.
    pub fn rejected(note: impl Into<String>) -> Self {
        Self {
            outcome: "rejected".into(),
            note: note.into(),
        }
    }

    /// True when approved.
    pub fn is_approved(&self) -> bool {
        self.outcome == "approved"
    }
}

/// The durable function's input: a workflow + the trigger payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunInput {
    /// The workflow definition (serialized; replayed on resume).
    pub workflow: Workflow,
    /// The trigger payload (available to `{{trigger.*}}` templates).
    pub trigger: serde_json::Value,
}

/// The shared action-execution surface (the Mailbox). Run by the workflow
/// function via a Resonate dependency (consistency rule).
///
/// Every action returns a [`serde_json::Value`] (ADR-0005 Spec 0): the value
/// flows into `steps.<name>` for downstream templates/guards. `Null` is a
/// valid "no output" action.
pub type ActionRunner = Arc<dyn Fn(&Action) -> anyhow::Result<serde_json::Value> + Send + Sync>;

/// The `spawn` profile resolver DI seam (ADR-0005 Spec 4a). Resolves a
/// profile name (`@housekeeper`) to the (npub, spawn spec) the embedding
/// spawns. The ENGINE never imports flywheel — this seam is injected, same
/// idiom as `ActionRunner`. `None` → unknown profile (fail the step).
pub type ProfileResolver =
    Arc<dyn Fn(&str) -> Option<serde_json::Value> + Send + Sync>;

/// Optional side-channel for approval promise-ids (embedding/test hook). When
/// a run parks on `request_approval`, its promise_id is pushed here so the
/// embedding can publish the kind-46011 pending event.
///
/// Distinct newtype (not a `type` alias): the Resonate dependency map keys by
/// `TypeId` of the value type, so two `Arc<Mutex<Vec<String>>>` aliases would
/// collide and `get_dependency` would resolve both sinks to the LAST
/// registered — a real 0.6-migration regression (a1fe947). The newtype gives
/// each sink a unique `TypeId`; `Deref` keeps the `.lock().await` call sites
/// unchanged.
#[derive(Clone, Default)]
pub struct ApprovalSink(Arc<tokio::sync::Mutex<Vec<String>>>);

impl std::ops::Deref for ApprovalSink {
    type Target = tokio::sync::Mutex<Vec<String>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Side-channel for `spawn` promise-ids (ADR-0005 Spec 4a). When a run parks
/// on a spawn-await, its promise_id is pushed here so the embedding can emit
/// the kind-43001 job carrying it in the `spec`.
///
/// Distinct newtype — see [`ApprovalSink`] for why (TypeId collision in the
/// Resonate dependency map).
#[derive(Clone, Default)]
pub struct SpawnSink(Arc<tokio::sync::Mutex<Vec<String>>>);

impl std::ops::Deref for SpawnSink {
    type Target = tokio::sync::Mutex<Vec<String>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// The workflow engine: owns the Resonate runtime + registers the durable
/// function + the approval resolution bridge.
pub struct WorkflowEngine {
    resonate: Arc<Resonate>,
    approval_sink: ApprovalSink,
    spawn_sink: SpawnSink,
    policy_sink: crate::cron_failure::PolicySink,
}

impl std::fmt::Debug for WorkflowEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowEngine")
            .field("resonate", &"<runtime>")
            .field(
                "approval_sink_len",
                &self.approval_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .field(
                "spawn_sink_len",
                &self.spawn_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .field(
                "policy_sink_len",
                &self.policy_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl WorkflowEngine {
    /// New engine over an in-process Resonate runtime (`Resonate::local`),
    /// with the shared action runner + an approval sink injected.
    pub fn local(action_runner: ActionRunner) -> anyhow::Result<Self> {
        Self::local_with(action_runner, Self::default_resolver())
    }

    /// Like [`Self::local`] with a `ProfileResolver` for the `spawn` action
    /// (ADR-0005 Spec 4a DI seam).
    pub fn local_with(
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
    ) -> anyhow::Result<Self> {
        let sink = ApprovalSink::default();
        let spawn_sink = SpawnSink::default();
        let policy_sink = crate::cron_failure::PolicySink::new();
        let resonate = Resonate::local()
            .with_dependency(action_runner)
            .with_dependency(profile_resolver)
            .with_dependency(sink.clone())
            .with_dependency(spawn_sink.clone())
            .with_dependency(policy_sink.clone());
        resonate
            .register(run_workflow)
            .map_err(|e| anyhow::anyhow!("register workflow function: {e}"))?;
        resonate
            .register(run_graph_node)
            .map_err(|e| anyhow::anyhow!("register graph node function: {e}"))?;
        resonate
            .register(run_parallel_copy)
            .map_err(|e| anyhow::anyhow!("register parallel copy function: {e}"))?;
        Ok(Self {
            resonate: Arc::new(resonate),
            approval_sink: sink,
            spawn_sink,
            policy_sink,
        })
    }

    /// New engine over a remote Resonate server (`ResonateConfig.url`),
    /// enabling cross-process crash survival (SC-11 uses a real server).
    pub fn remote(url: impl Into<String>, action_runner: ActionRunner) -> anyhow::Result<Self> {
        Self::remote_with(url, action_runner, Self::default_resolver())
    }

    /// Like [`Self::remote`] with a `ProfileResolver` for the `spawn` action.
    pub fn remote_with(
        url: impl Into<String>,
        action_runner: ActionRunner,
        profile_resolver: ProfileResolver,
    ) -> anyhow::Result<Self> {
        let sink = ApprovalSink::default();
        let spawn_sink = SpawnSink::default();
        let policy_sink = crate::cron_failure::PolicySink::new();
        let config = ResonateConfig {
            url: Some(url.into()),
            ..Default::default()
        };
        let resonate = Resonate::new(config)
            .with_dependency(action_runner)
            .with_dependency(profile_resolver)
            .with_dependency(sink.clone())
            .with_dependency(spawn_sink.clone())
            .with_dependency(policy_sink.clone());
        resonate
            .register(run_workflow)
            .map_err(|e| anyhow::anyhow!("register workflow function: {e}"))?;
        resonate
            .register(run_graph_node)
            .map_err(|e| anyhow::anyhow!("register graph node function: {e}"))?;
        resonate
            .register(run_parallel_copy)
            .map_err(|e| anyhow::anyhow!("register parallel copy function: {e}"))?;
        Ok(Self {
            resonate: Arc::new(resonate),
            approval_sink: sink,
            spawn_sink,
            policy_sink,
        })
    }

    /// The default `ProfileResolver`: every profile is unknown (the embedding
    /// must inject a real one for `spawn` to succeed).
    fn default_resolver() -> ProfileResolver {
        Arc::new(|_name: &str| None)
    }

    /// The Resonate runtime (for the approval listener to resolve promises).
    pub fn resonate(&self) -> &Arc<Resonate> {
        &self.resonate
    }

    /// The approval side-channel: promise-ids of parked approvals.
    pub fn approval_sink(&self) -> &ApprovalSink {
        &self.approval_sink
    }

    /// The spawn side-channel: promise-ids of parked spawn-awaits (the
    /// embedding reads these to emit the kind-43001 job carrying them).
    pub fn spawn_sink(&self) -> &SpawnSink {
        &self.spawn_sink
    }

    /// The cron failure-policy side-channel (ADR-0005 Spec 3, P4.4): policy
    /// decisions recorded by the cron loop, executed by the embedding (bead /
    /// escalate DM).
    pub fn policy_sink(&self) -> &crate::cron_failure::PolicySink {
        &self.policy_sink
    }

    /// Start a workflow run, idempotent on `run_id` (a redelivered trigger
    /// must not double-start — Resonate dedups on the run id).
    pub async fn run(&self, run_id: &str, input: RunInput) -> anyhow::Result<serde_json::Value> {
        self.resonate
            .run(run_id, run_workflow, input)
            .await
            .map_err(|e| anyhow::anyhow!("resonate run {run_id}: {e}"))
    }

    /// On-demand invoke (ADR-0005 Spec 2, P3): run a workflow with NO trigger
    /// event. The engine synthesizes a `trigger.*` template context from the
    /// provided `inputs` so `{{inputs.x}}` / `{{trigger.x}}` resolve on the
    /// invoke path — the SAME durable function as the trigger path, two entry
    /// doors.
    pub async fn invoke(
        &self,
        run_id: &str,
        workflow: Workflow,
        inputs: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        // Synthesized trigger payload: the inputs are visible both as
        // `{{inputs.<name>}}` (under `inputs`) and `{{trigger.<name>}}`
        // (flattened at the top level).
        let mut trigger = serde_json::Map::new();
        if let Some(obj) = inputs.as_object() {
            for (k, v) in obj {
                trigger.insert(k.clone(), v.clone());
            }
        }
        trigger.insert("inputs".into(), inputs.clone());
        let input = RunInput {
            workflow,
            trigger: serde_json::Value::Object(trigger),
        };
        self.run(run_id, input).await
    }

    /// Resolve an approval promise (the kind-46011 listener calls this).
    pub async fn resolve_approval(
        &self,
        promise_id: &str,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        let value = Value::from_serializable(decision)
            .map_err(|e| anyhow::anyhow!("serialize decision: {e}"))?;
        self.resonate
            .promises
            .resolve(promise_id, value)
            .await
            .map_err(|e| anyhow::anyhow!("resolve promise {promise_id}: {e}"))?;
        Ok(())
    }

    /// Reject an approval promise.
    pub async fn reject_approval(
        &self,
        promise_id: &str,
        decision: &Decision,
    ) -> anyhow::Result<()> {
        let value = Value::from_serializable(decision)
            .map_err(|e| anyhow::anyhow!("serialize decision: {e}"))?;
        self.resonate
            .promises
            .reject(promise_id, value)
            .await
            .map_err(|e| anyhow::anyhow!("reject promise {promise_id}: {e}"))?;
        Ok(())
    }

    /// Resolve a spawn-await promise (the bus subscriber calls this on
    /// kind-43004 result — ADR-0005 Spec 4a).
    pub async fn resolve_spawn(
        &self,
        promise_id: &str,
        value: serde_json::Value,
    ) -> anyhow::Result<()> {
        let v = Value::from_serializable(value)
            .map_err(|e| anyhow::anyhow!("serialize spawn result: {e}"))?;
        self.resonate
            .promises
            .resolve(promise_id, v)
            .await
            .map_err(|e| anyhow::anyhow!("resolve spawn promise {promise_id}: {e}"))?;
        Ok(())
    }

    /// Reject a spawn-await promise (the bus subscriber calls this on
    /// kind-43006 error — the job FAILED, so the run fails via Spec 0).
    pub async fn reject_spawn(&self, promise_id: &str, error: &str) -> anyhow::Result<()> {
        let value = Value::from_serializable(serde_json::json!({ "error": error }))
            .map_err(|e| anyhow::anyhow!("serialize spawn error: {e}"))?;
        self.resonate
            .promises
            .reject(promise_id, value)
            .await
            .map_err(|e| anyhow::anyhow!("reject spawn promise {promise_id}: {e}"))?;
        Ok(())
    }
}

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
    if let Some(failed) = run_steps(ctx, &runner, &input.trigger, &workflow.steps, &mut step_outputs).await? {
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
async fn run_steps(
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
fn run_step<'a>(
    ctx: &'a Context,
    runner: &'a ActionRunner,
    trigger: &'a serde_json::Value,
    step: &'a Step,
    step_outputs: &'a mut serde_json::Map<String, serde_json::Value>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = resonate_sdk::prelude::Result<serde_json::Value>> + Send + 'a>> {
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
                    return Err(resonate_sdk::error::Error::Suspended)
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
        Action::Retry { max_attempts, backoff_secs, step: inner } => {
            run_retry(ctx, runner, trigger, inner, *max_attempts, *backoff_secs).await?
        }
        Action::Branch { if_guard, then, else_steps } => {
            run_branch(ctx, runner, trigger, if_guard, then, else_steps).await?
        }
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
        } => run_spawn(ctx, profile, workflow.as_deref(), *await_, *no_claim_timeout).await?,
    };

    // P0.2 (ADR-0005 Spec 0): `on_error: fail_run` converts a step failure
    // into a RUN failure — a deterministic durable RETURN, never a panic.
    if step.on_error == StepOnError::FailRun
        && value.get("ok").and_then(serde_json::Value::as_bool) == Some(false)
    {
        let error = value["error"]
            .as_str()
            .unwrap_or("step failed")
            .to_string();
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
async fn run_retry(
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
        last_err = value.get("error").and_then(|e| e.as_str()).map(String::from);
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
async fn run_branch(
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
async fn run_foreach(
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
        // Each iteration sees the current item as `{{item}}`.
        let mut item_outputs = serde_json::Map::new();
        let value = run_step(ctx, runner, trigger, each, &mut item_outputs).await?;
        let _ = item;
        collected.push(value);
    }
    Ok(serde_json::Value::Array(collected))
}

/// Run a `parallel` action: fan `each` out across a crew role (`over:
/// "@crew.r"`) or N elastic copies (`over: "<N>"`). For elastic N, spawns N
/// concurrent sub-runs of `each`; collected into a list. Crew-role fan-out
/// (the orchestrator steers the actual spawned agents) is resolved by the
/// embedder via the `ProfileResolver` DI seam — here N elastic copies run.
async fn run_parallel(
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
async fn run_call_workflow(
    ctx: &Context,
    runner: &ActionRunner,
    workflow: &str,
    inputs: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> resonate_sdk::prelude::Result<serde_json::Value> {
    let _ = (ctx, runner, workflow, inputs, step_outputs);
    // The full sub-run wiring (a separate WorkflowEngine embedded in the same
    // runtime, or a nested Resonate run) lands with the call_workflow schema
    // bead. Until then this is a structural placeholder that records intent.
    Ok(serde_json::json!({
        "ok": false,
        "error": "call_workflow sub-runs land in the P5.3 wiring step",
    }))
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
async fn run_spawn(
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
            }))
        }
    };

    // 2. Create the promise — the job's binding key.
    let spec = serde_json::json!({
        "profile": profile,
        "workflow": workflow,
        "resolved": resolved,
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
fn resolve_template_ref(
    token: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<serde_json::Value> {
    let trimmed = token.trim();
    if let Some(inner) = trimmed
        .strip_prefix("{{")
        .and_then(|s| s.strip_suffix("}}"))
    {
        return resolve_template(inner, trigger, step_outputs)
            .and_then(|s| serde_json::from_str(&s).ok())
            .or_else(|| resolve_template(inner, trigger, step_outputs).map(serde_json::Value::String));
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
async fn run_graph(
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
            let needs_satisfied = node
                .needs
                .iter()
                .all(|need| done.contains(need));
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
                .run(run_graph_node, GraphNodeInput { action: rendered })
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
    let _runner = ctx.get_dependency::<ActionRunner>();
    match (_runner)(&input.action) {
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
    if tags
        .iter()
        .any(|t| t.as_slice().get(0).map(String::as_str) == Some("t")
            && t.as_slice().get(1).map(String::as_str) == Some(WORKFLOW_EMITTED_TAG))
    {
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

/// Substitute `{{trigger.<path>}}` / `{{steps.<name>.<path>}}` templates in an
/// action's string fields before the runner sees them (ADR-0005 Spec 0:
/// value-returning actions flow into `steps.<name>` for downstream templates).
///
/// Unresolvable tokens stay verbatim (backward-compatible — a body that
/// references a step that didn't produce output keeps its literal text).
fn render_action(
    action: &Action,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> anyhow::Result<Action> {
    let mut value = serde_json::to_value(action)?;
    render_json_value(&mut value, trigger, step_outputs);
    serde_json::from_value(value)
        .map_err(|e| anyhow::anyhow!("re-deserialize rendered action: {e}"))
}

/// Recursively render template tokens in every string leaf of a JSON value.
fn render_json_value(
    value: &mut serde_json::Value,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) {
    match value {
        serde_json::Value::String(s) => {
            *s = render_template(s, trigger, step_outputs);
        }
        serde_json::Value::Array(items) => {
            for item in items {
                render_json_value(item, trigger, step_outputs);
            }
        }
        serde_json::Value::Object(map) => {
            for item in map.values_mut() {
                render_json_value(item, trigger, step_outputs);
            }
        }
        _ => {}
    }
}

/// Replace every `{{...}}` token in `s` with the value it names
/// (`steps.<name>.<path>` or `trigger.<path>`), rendered as JSON text.
/// Unresolvable tokens are left verbatim.
fn render_template(
    s: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        match after.find("}}") {
            Some(end) => {
                let token = &after[..end];
                if let Some(replaced) = resolve_template(token.trim(), trigger, step_outputs) {
                    out.push_str(&replaced);
                } else {
                    // Unresolvable — keep the literal token.
                    out.push_str(&format!("{{{{{token}}}}}"));
                }
                rest = &after[end + 2..];
            }
            None => {
                out.push_str(&rest[start..]);
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Resolve one template token to a rendered string, or `None` when the path
/// does not resolve (caller keeps the literal).
fn resolve_template(
    token: &str,
    trigger: &serde_json::Value,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let (root, path) = token.split_once('.')?;
    let value = match root {
        "trigger" => trigger,
        // `{{inputs.x}}`: the invoke path embeds the caller's inputs under
        // `trigger.inputs` (invoke synthesizes the trigger payload from them).
        // Resolve the bare `inputs` root too — `{{inputs.goal}}` ==
        // `{{trigger.inputs.goal}}` on the invoke path.
        "inputs" => {
            let inputs = trigger.get("inputs")?;
            if path.is_empty() {
                return Some(render_scalar(inputs));
            }
            return descend(inputs, path.split('.')).map(render_scalar);
        }
        "steps" => return resolve_steps(&path, step_outputs),
        _ => return None,
    };
    descend(value, path.split('.')).map(|v| render_scalar(v))
}

/// Resolve `steps.<name>.<path>…` (the whole object when no `<path>`).
fn resolve_steps(
    path: &str,
    step_outputs: &serde_json::Map<String, serde_json::Value>,
) -> Option<String> {
    let (name, rest) = match path.split_once('.') {
        Some((name, rest)) => (name, rest),
        None => (path, ""),
    };
    let value = step_outputs.get(name)?;
    if rest.is_empty() {
        return Some(render_scalar(value));
    }
    descend(value, rest.split('.')).map(render_scalar)
}

/// Walk a dotted path through a JSON value.
fn descend<'a>(
    mut value: &'a serde_json::Value,
    mut path: std::str::Split<'_, char>,
) -> Option<&'a serde_json::Value> {
    while let Some(part) = path.next() {
        value = value.get(part)?;
    }
    Some(value)
}

/// Render a resolved value as template text: scalars verbatim, structures as
/// compact JSON.
fn render_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// P5.3: `foreach` — iterate over a list; collect sub-run outputs.
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
    over: "[1,2,3]"
    each:
      action: send_dm
      to: pi
      body: item
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

    /// P5.3: `call_workflow` — the structural placeholder records intent.
    #[tokio::test]
    async fn call_workflow_subrun_reports_intent() {
        let engine = WorkflowEngine::local(noop_runner()).expect("engine");
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
        // The wiring step returns an intent marker (sub-run wiring lands with
        // the embedded-engine bead). The action parsed + ran.
        assert!(output["sub"].is_object(), "call_workflow returned intent: {output}");
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
}
