// governed-by: ADR-0043
#![forbid(unsafe_code)]

//! The engine's in-process durable driver (bd-engine-facade-74nw).
//!
//! `WorkflowEngine::run` is the public door; this module is what happens
//! behind it: open the durable run, drive its effects through the injected
//! seams, and return the terminal value. Approval and spawn parks become
//! sink entries plus oneshot waiters, so the drainer thread (flywheel, the
//! MCP host) resolves them exactly as it resolved vendor promises before
//! ADR-0043 — the promise id is now the run id / spawn key.
//!
//! No vendor runtime participates. Time is `tokio::time`; the run model owns
//! progress. A `{status:"failed",…}` terminal is a VALUE, not an `Err`.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::sync::oneshot;

use toron_core::index::durable_workflow::RunState;
use toron_core::kernel::EngineStorage;

use super::durable::{
    GraphEffect, GraphModel, RunEffect, RunPhase as ModelPhase, needs_graph_model,
};
use super::durable_host::{DurableGraphRun, DurableRun, HostError};
use super::leaf::LeafSeams;
use super::linear_step::{GraphNodeStep, LinearStep, run_graph_node_step, run_linear_step};
use super::spawn::SpawnLauncher;
use super::{ApprovalSink, SpawnSink, WorkflowResolver};
use crate::schema::Workflow;

/// One waiting approval/spawn signal, keyed by the id pushed to the sink.
#[derive(Clone, Default)]
pub struct Waiters(Arc<Mutex<BTreeMap<String, oneshot::Sender<Value>>>>);

impl std::fmt::Debug for Waiters {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Waiters")
            .field("len", &self.lock().len())
            .finish()
    }
}

impl Waiters {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, oneshot::Sender<Value>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Register one waiter and return its receiver. Registering BEFORE the
    /// sink push closes the race with a fast drainer.
    pub fn register(&self, id: &str) -> oneshot::Receiver<Value> {
        let (tx, rx) = oneshot::channel();
        self.lock().insert(id.to_string(), tx);
        rx
    }

    /// Signal one waiter. `false` when nothing waits under that id.
    pub fn signal(&self, id: &str, value: Value) -> bool {
        let Some(tx) = self.lock().remove(id) else {
            return false;
        };
        tx.send(value).is_ok()
    }

    /// Drop every waiter (the process-scoped `stop`).
    pub fn cancel_all(&self) {
        self.lock().clear();
    }
}

/// Drive one child run by id. The engine supplies this; the graph driver calls
/// it on `ChildRun` and feeds the terminal back to the parent.
pub type ChildDriver = Arc<
    dyn Fn(String, Workflow, Value) -> Pin<Box<dyn Future<Output = anyhow::Result<Value>> + Send>>
        + Send
        + Sync,
>;

/// Everything a run needs from the engine, as data.
#[derive(Clone)]
pub struct DriveSeams {
    /// Leaf policy (runners, verify sink, journal mode).
    pub leaf: LeafSeams,
    /// Resolver for `call_workflow` children.
    pub workflows: WorkflowResolver,
    /// The engine's spawn launcher: register a waiter, announce the key on the
    /// sink, await the consumer.
    pub spawn: SpawnLauncher,
    /// Approval waiters (shared with the engine).
    pub approvals: Waiters,
    /// Where parked ids are announced.
    pub approval_sink: ApprovalSink,
    /// Where spawn keys are announced.
    pub spawn_sink: SpawnSink,
    /// The spec behind each announced key (the embedding's drainer reads it).
    pub spawn_specs: Arc<Mutex<BTreeMap<String, super::spawn::SpawnSpec>>>,
    /// Child runs.
    pub children: ChildDriver,
    /// Set by `stop()`; a run checks it at every effect boundary.
    pub stop: Arc<AtomicBool>,
}

impl std::fmt::Debug for DriveSeams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriveSeams").finish_non_exhaustive()
    }
}

/// Host time, unix millis.
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Run one sync driver verb off the async worker.
async fn verb<T, F>(f: F) -> anyhow::Result<T>
where
    F: FnOnce() -> Result<T, HostError> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| anyhow::anyhow!("durable verb task: {e}"))?
        .map_err(|e| anyhow::anyhow!("durable verb: {e}"))
}

fn stopped(seams: &DriveSeams) -> bool {
    seams.stop.load(Ordering::SeqCst)
}

/// Drive a linear durable run to its terminal value.
pub(crate) async fn drive_linear<S, J, R>(
    mut run: DurableRun<S, J, R>,
    mut effects: Vec<RunEffect>,
    seams: &DriveSeams,
) -> anyhow::Result<Value>
where
    S: EngineStorage + Send + 'static,
    J: toron_core::kernel::JournalStore + Send + 'static,
    R: RunState + Send + 'static,
{
    let run_id = run.model().run_id.clone();
    let workflow = run.model().workflow.name.clone();
    let digest = crate::identity::workflow_digest(&run.model().workflow);
    let trigger = run.model().trigger.clone();
    let mut outputs: serde_json::Map<String, Value> =
        run.model().outputs.clone().into_iter().collect();
    loop {
        if stopped(seams) {
            anyhow::bail!("run {run_id} stopped");
        }
        if effects.is_empty() {
            if run.model().is_terminal() {
                return Ok(linear_terminal_value(run.model()));
            }
            anyhow::bail!("run {run_id}: durable drive ran out of effects while non-terminal");
        }
        let mut follow = Vec::new();
        for effect in std::mem::take(&mut effects) {
            match effect {
                RunEffect::ExecuteStep {
                    index,
                    step,
                    iteration,
                } => {
                    let value = run_linear_step(
                        LinearStep {
                            run_id: &run_id,
                            workflow: &workflow,
                            workflow_digest: &digest,
                            step: &step,
                            index,
                            trigger: &trigger,
                            outputs: &mut outputs,
                            iteration,
                        },
                        &seams.leaf,
                    )
                    .await;
                    let at = now_ms();
                    let (back, mut more) = verb(move || {
                        let mut run = run;
                        let effects = run.step_done(value, at)?;
                        Ok::<_, HostError>((run, effects))
                    })
                    .await?;
                    run = back;
                    outputs = run.model().outputs.clone().into_iter().collect();
                    follow.append(&mut more);
                }
                RunEffect::ParkUntil { until_ms, .. } => {
                    // A spawn-await emits its deadline park in the SAME batch
                    // as StartSpawn. Once the settle lands the model is no
                    // longer parked, so that effect is stale: sleeping on it
                    // would hold the run for the whole `no_claim_timeout`.
                    if !matches!(run.model().phase, ModelPhase::Parked { .. }) {
                        continue;
                    }
                    let now = now_ms();
                    if until_ms > now {
                        tokio::time::sleep(Duration::from_millis(until_ms - now)).await;
                    }
                    let at = now_ms();
                    let (back, mut more) = verb(move || {
                        let mut run = run;
                        let effects = run.woke(at)?;
                        Ok::<_, HostError>((run, effects))
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("run {run_id}: wake failed: {e}"))?;
                    run = back;
                    follow.append(&mut more);
                }
                RunEffect::AwaitApproval { step, message } => {
                    let wait_id = run_id.clone();
                    let rx = seams.approvals.register(&wait_id);
                    seams.approval_sink.lock().await.push(wait_id.clone());
                    let _ = message;
                    let decided = rx
                        .await
                        .map_err(|_| anyhow::anyhow!("run {run_id}: approval {step} cancelled"))?;
                    let at = now_ms();
                    let (back, mut more) = verb(move || {
                        let mut run = run;
                        let effects = run.approve(decided, at)?;
                        Ok::<_, HostError>((run, effects))
                    })
                    .await
                    .map_err(|e| anyhow::anyhow!("run {run_id}: approval failed: {e}"))?;
                    run = back;
                    follow.append(&mut more);
                }
                RunEffect::StartSpawn { node, spec } => {
                    let deadline = match run.model().phase {
                        ModelPhase::AwaitingSpawn { deadline_ms, .. } => deadline_ms,
                        _ => None,
                    };
                    let outcome = await_spawn_launch(&seams.spawn, &spec, deadline).await;
                    let at = now_ms();
                    let (back, mut more) = match outcome {
                        SpawnLaunch::Settled(value) => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.spawn_settled(&node, value, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!("run {run_id}: spawn settle failed: {e}")
                            })?
                        }
                        SpawnLaunch::Failed(error) => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.spawn_failed(&node, error, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: spawn fail failed: {e}"))?
                        }
                        SpawnLaunch::TimedOut => {
                            // The fold owns the canonical timeout text.
                            let at = deadline.unwrap_or_else(now_ms).max(now_ms());
                            verb(move || {
                                let mut run = run;
                                let effects = run.woke(at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!("run {run_id}: spawn timeout failed: {e}")
                            })?
                        }
                    };
                    run = back;
                    follow.append(&mut more);
                }
                RunEffect::SpawnDetached { spec, .. } => {
                    // Fire and forget: the job is announced, the run does not
                    // wait. The consumer owns the terminal; the spec rides the
                    // engine's map so the drainer can launch it.
                    seams
                        .spawn_specs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(spec.spawn_key.clone(), spec.clone());
                    seams.spawn_sink.lock().await.push(spec.spawn_key.clone());
                }
                RunEffect::Complete { output } => {
                    // The engine's public contract returns the STEP MAP on
                    // success (the old `run_workflow` shape); the envelope is
                    // the model's internal record.
                    return Ok(output.get("steps").cloned().unwrap_or(output));
                }
                RunEffect::Fail { output, .. } => return Ok(output),
                RunEffect::Cancelled => return Ok(json!({"status": "cancelled"})),
                RunEffect::Unsupported { reason } => {
                    anyhow::bail!("run {run_id}: unsupported shape: {reason}")
                }
            }
        }
        effects = follow;
    }
}

/// One launch attempt's outcome, bounded by the model's park deadline.
enum SpawnLaunch {
    Settled(Value),
    Failed(String),
    TimedOut,
}

/// Await the engine's spawn launcher, bounded by the model deadline.
async fn await_spawn_launch(
    launcher: &SpawnLauncher,
    spec: &super::spawn::SpawnSpec,
    deadline_ms: Option<u64>,
) -> SpawnLaunch {
    let Some(deadline) = deadline_ms else {
        return match launcher(spec.clone()).await {
            Ok(value) => SpawnLaunch::Settled(value),
            Err(error) => SpawnLaunch::Failed(format!("{error:#}")),
        };
    };
    let now = now_ms();
    if deadline <= now {
        return SpawnLaunch::TimedOut;
    }
    match tokio::time::timeout(
        Duration::from_millis(deadline - now),
        launcher(spec.clone()),
    )
    .await
    {
        Ok(Ok(value)) => SpawnLaunch::Settled(value),
        Ok(Err(error)) => SpawnLaunch::Failed(format!("{error:#}")),
        Err(_) => SpawnLaunch::TimedOut,
    }
}

/// Drive a graph durable run to its terminal value.
pub(crate) async fn drive_graph<S, J, R>(
    mut run: DurableGraphRun<S, J, R>,
    mut effects: Vec<GraphEffect>,
    seams: &DriveSeams,
) -> anyhow::Result<Value>
where
    S: EngineStorage + Send + 'static,
    J: toron_core::kernel::JournalStore + Send + 'static,
    R: RunState + Send + 'static,
{
    let run_id = run.model().run_id.clone();
    loop {
        if stopped(seams) {
            anyhow::bail!("run {run_id} stopped");
        }
        if effects.is_empty() {
            if run.model().is_terminal() {
                return Ok(terminal_value(run.model()));
            }
            anyhow::bail!("run {run_id}: durable graph drive stopped without a terminal");
        }
        let mut follow = Vec::new();
        for effect in std::mem::take(&mut effects) {
            match effect {
                GraphEffect::ExecuteNode {
                    node,
                    wave: _,
                    attempt,
                    inputs,
                    item,
                } => {
                    let Some(node_def) = run
                        .model()
                        .workflow
                        .graph
                        .as_ref()
                        .and_then(|g| g.nodes.get(&node))
                        .cloned()
                    else {
                        anyhow::bail!("run {run_id}: graph effect names unknown node {node}");
                    };
                    let snapshot: serde_json::Map<String, Value> = inputs.into_iter().collect();
                    let value = run_graph_node_step(
                        GraphNodeStep {
                            run_id: &run_id,
                            workflow: &run.model().workflow.name,
                            workflow_digest: &crate::identity::workflow_digest(
                                &run.model().workflow,
                            ),
                            node_name: &node,
                            node: &node_def,
                            attempt,
                            trigger: &run.model().trigger,
                            inputs: &snapshot,
                            item: item.as_ref(),
                        },
                        &seams.leaf,
                    )
                    .await;
                    let at = now_ms();
                    let feed = node.clone();
                    let (back, mut more) = verb(move || {
                        let mut run = run;
                        let effects = run.node_done(&feed, value, at)?;
                        Ok::<_, HostError>((run, effects))
                    })
                    .await?;
                    run = back;
                    follow.append(&mut more);
                }
                GraphEffect::ParkUntil { until_ms, .. } => {
                    let now = now_ms();
                    if until_ms > now {
                        tokio::time::sleep(Duration::from_millis(until_ms - now)).await;
                    }
                    let at = now_ms();
                    let (back, mut more) = verb(move || {
                        let mut run = run;
                        let effects = run.woke(at)?;
                        Ok::<_, HostError>((run, effects))
                    })
                    .await?;
                    run = back;
                    follow.append(&mut more);
                }
                GraphEffect::AwaitApproval { node, .. } => {
                    let wait_id = format!("{run_id}:{node}");
                    let rx = seams.approvals.register(&wait_id);
                    seams.approval_sink.lock().await.push(wait_id.clone());
                    let decided = rx
                        .await
                        .map_err(|_| anyhow::anyhow!("run {run_id}: approval {node} cancelled"))?;
                    let at = now_ms();
                    let feed = node.clone();
                    let (back, mut more) = verb(move || {
                        let mut run = run;
                        let effects = run.approve(&feed, decided, at)?;
                        Ok::<_, HostError>((run, effects))
                    })
                    .await?;
                    run = back;
                    follow.append(&mut more);
                }
                GraphEffect::ChildRun {
                    node,
                    child_run_id,
                    workflow,
                    trigger,
                } => {
                    let outcome = match (seams.workflows)(&workflow) {
                        Some(child) => (seams.children)(child_run_id.clone(), child, trigger).await,
                        None => Err(anyhow::anyhow!("unknown workflow: {workflow}")),
                    };
                    let at = now_ms();
                    let feed = node.clone();
                    let (back, mut more) = match outcome {
                        Ok(value) => {
                            verb(move || {
                                let mut run = run;
                                let effects = run.child_succeeded(&feed, value, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await?
                        }
                        Err(error) => {
                            let value = json!({"ok": false, "error": format!("{error:#}")});
                            verb(move || {
                                let mut run = run;
                                let effects = run.child_failed(&feed, value, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await?
                        }
                    };
                    run = back;
                    follow.append(&mut more);
                }
                GraphEffect::ChildJoin { .. } => {}
                GraphEffect::Complete { output } => return Ok(output),
                GraphEffect::Fail { fail } => return Ok(fail.to_value()),
                GraphEffect::Cancelled => return Ok(json!({"status": "cancelled"})),
                GraphEffect::Unsupported { reason } => {
                    anyhow::bail!("run {run_id}: unsupported shape: {reason}")
                }
            }
        }
        effects = follow;
    }
}

/// The terminal value of an already-finished graph model.
fn linear_terminal_value(model: &super::durable::RunModel) -> Value {
    match &model.phase {
        super::durable::RunPhase::Succeeded => json!({
            "status": "succeeded",
            "steps": model.outputs,
        }),
        super::durable::RunPhase::Failed { error } => json!({
            "status": "failed",
            "error": error,
            "failed_step": "<workflow>",
        }),
        super::durable::RunPhase::Cancelled => json!({"status": "cancelled"}),
        _ => Value::Null,
    }
}

fn terminal_value(model: &GraphModel) -> Value {
    match &model.phase {
        super::durable::GraphPhase::Succeeded => Value::Object(
            model
                .outputs
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        ),
        super::durable::GraphPhase::Failed { error } => json!({
            "status": "failed",
            "error": error,
            "failed_step": "<graph>",
        }),
        super::durable::GraphPhase::Cancelled => json!({"status": "cancelled"}),
        _ => Value::Null,
    }
}

/// True when the shape runs on the graph driver.
pub(crate) fn uses_graph(workflow: &Workflow) -> bool {
    needs_graph_model(workflow)
}

/// In-memory model seam for a local engine (bd-engine-facade-74nw).
#[derive(Clone, Default)]
pub struct MemRunState(Arc<std::sync::Mutex<BTreeMap<String, String>>>);

impl RunState for MemRunState {
    fn load(&self, run_id: &str) -> anyhow::Result<Option<String>> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(run_id)
            .cloned())
    }

    fn save(&self, run_id: &str, state_json: &str) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(run_id.to_string(), state_json.to_string());
        Ok(())
    }

    fn delete(&self, run_id: &str) -> anyhow::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(run_id);
        Ok(())
    }

    fn list(&self) -> anyhow::Result<Vec<(String, String)>> {
        Ok(self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}
