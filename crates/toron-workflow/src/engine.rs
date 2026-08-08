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

use resonate_sdk::prelude::{Resonate, ResonateConfig};
use resonate_sdk::types::Value;

use crate::schema::{Action, Workflow};

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

/// Optional side-channel for approval promise-ids (embedding/test hook). When
/// a run parks on `request_approval`, its promise_id is pushed here so the
/// embedding can publish the kind-46011 pending event.
pub type ApprovalSink = Arc<tokio::sync::Mutex<Vec<String>>>;

/// The workflow engine: owns the Resonate runtime + registers the durable
/// function + the approval resolution bridge.
pub struct WorkflowEngine {
    resonate: Arc<Resonate>,
    approval_sink: ApprovalSink,
}

impl std::fmt::Debug for WorkflowEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowEngine")
            .field("resonate", &"<runtime>")
            .field(
                "approval_sink_len",
                &self.approval_sink.try_lock().map(|g| g.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl WorkflowEngine {
    /// New engine over an in-process Resonate runtime (`Resonate::local`),
    /// with the shared action runner + an approval sink injected.
    pub fn local(action_runner: ActionRunner) -> anyhow::Result<Self> {
        let sink: ApprovalSink = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let resonate = Resonate::local()
            .with_dependency(action_runner)
            .with_dependency(sink.clone());
        resonate
            .register(run_workflow)
            .map_err(|e| anyhow::anyhow!("register workflow function: {e}"))?;
        Ok(Self {
            resonate: Arc::new(resonate),
            approval_sink: sink,
        })
    }

    /// New engine over a remote Resonate server (`ResonateConfig.url`),
    /// enabling cross-process crash survival (SC-11 uses a real server).
    pub fn remote(url: impl Into<String>, action_runner: ActionRunner) -> anyhow::Result<Self> {
        let sink: ApprovalSink = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let config = ResonateConfig {
            url: Some(url.into()),
            ..Default::default()
        };
        let resonate = Resonate::new(config)
            .with_dependency(action_runner)
            .with_dependency(sink.clone());
        resonate
            .register(run_workflow)
            .map_err(|e| anyhow::anyhow!("register workflow function: {e}"))?;
        Ok(Self {
            resonate: Arc::new(resonate),
            approval_sink: sink,
        })
    }

    /// The Resonate runtime (for the approval listener to resolve promises).
    pub fn resonate(&self) -> &Arc<Resonate> {
        &self.resonate
    }

    /// The approval side-channel: promise-ids of parked approvals.
    pub fn approval_sink(&self) -> &ApprovalSink {
        &self.approval_sink
    }

    /// Start a workflow run, idempotent on `run_id` (a redelivered trigger
    /// must not double-start — Resonate dedups on the run id).
    pub async fn run(&self, run_id: &str, input: RunInput) -> anyhow::Result<serde_json::Value> {
        self.resonate
            .run(run_id, run_workflow, input)
            .await
            .map_err(|e| anyhow::anyhow!("resonate run {run_id}: {e}"))
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
    let mut step_outputs: serde_json::Map<String, serde_json::Value> = serde_json::Map::new();

    for step in &workflow.steps {
        let value = match &step.action {
            Action::Delay { secs } => {
                // Durable sleep — UNBOUNDED (the Buzz 270s cap is gone;
                // hours/days/weeks for SLA timers, cron-like cadence).
                ctx.sleep(Duration::from_secs(*secs)).await?;
                serde_json::Value::Null
            }
            Action::RequestApproval { message } => {
                // Create the promise; its id is the approval key.
                let pending = ctx
                    .promise::<Decision>()
                    .data(&serde_json::json!({ "message": message }))?;
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
                let decision: Decision = match pending.await {
                    Ok(d) => d,
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
                let value = match render_action(&step.action, &input.trigger, &step_outputs) {
                    Ok(action) => match (runner)(&action) {
                        Ok(value) => value,
                        Err(e) => serde_json::json!({ "ok": false, "error": e.to_string() }),
                    },
                    Err(e) => serde_json::json!({ "ok": false, "error": format!("render: {e}") }),
                };
                value
            }
        };
        if let Some(name) = &step.name {
            step_outputs.insert(name.clone(), value);
        }
    }

    Ok(serde_json::Value::Object(step_outputs))
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

    /// Poll the approval side-channel for a parked run's promise_id.
    async fn poll_promise_id(engine: &WorkflowEngine) -> String {
        for _ in 0..100 {
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
    #[tokio::test]
    async fn approval_suspends_and_resumes() {
        let runner = noop_runner();
        let engine = WorkflowEngine::local(runner.clone()).expect("engine");

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
    #[tokio::test]
    async fn approval_rejection_is_a_decision_not_a_failure() {
        let runner = noop_runner();
        let engine = WorkflowEngine::local(runner.clone()).expect("engine");
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
