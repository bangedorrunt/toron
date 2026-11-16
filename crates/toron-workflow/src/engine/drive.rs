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
    GraphEffect, GraphModel, JobWatchOutcome, JobWatchSpec, JobWatcher, NodeState, RunEffect,
    RunPhase as ModelPhase, await_job_watch, needs_graph_model,
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
    /// The engine's job watcher: watch one published 43001 for its
    /// 43002/43004/43006 terminal. `None` fails a WatchJob loud — the same
    /// fail-closed shape as a missing spawn launcher.
    pub job_watcher: Option<JobWatcher>,
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
                RunEffect::AwaitApproval {
                    step,
                    message,
                    deadline_ms,
                    ..
                } => {
                    let wait_id = run_id.clone();
                    let rx = seams.approvals.register(&wait_id);
                    seams.approval_sink.lock().await.push(wait_id.clone());
                    let _ = message;
                    let decided = match deadline_ms {
                        Some(deadline) => {
                            let wait = deadline.saturating_sub(now_ms());
                            match tokio::time::timeout(std::time::Duration::from_millis(wait), rx)
                                .await
                            {
                                Ok(Ok(v)) => Some(v),
                                Ok(Err(_)) => {
                                    return Err(anyhow::anyhow!(
                                        "run {run_id}: approval {step} cancelled"
                                    ));
                                }
                                Err(_) => None,
                            }
                        }
                        None => Some(rx.await.map_err(|_| {
                            anyhow::anyhow!("run {run_id}: approval {step} cancelled")
                        })?),
                    };
                    let at = now_ms();
                    let (back, mut more) = match decided {
                        Some(decided) => verb(move || {
                            let mut run = run;
                            let effects = run.approve(decided, at)?;
                            Ok::<_, HostError>((run, effects))
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("run {run_id}: approval failed: {e}"))?,
                        None => {
                            let deadline = deadline_ms.unwrap_or(at);
                            verb(move || {
                                let mut run = run;
                                let effects = run.woke(deadline)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!("run {run_id}: approval timeout failed: {e}")
                            })?
                        }
                    };
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
                RunEffect::WatchJob { node, job_id } => {
                    let (deadline, claimed) = match run.model().phase {
                        ModelPhase::AwaitingJob {
                            claim_deadline_ms,
                            result_deadline_ms,
                            claimed,
                            ..
                        } => {
                            if claimed {
                                (result_deadline_ms, true)
                            } else {
                                (claim_deadline_ms, false)
                            }
                        }
                        _ => (None, false),
                    };
                    let Some(watcher) = &seams.job_watcher else {
                        anyhow::bail!("run {run_id}: no job watcher: cannot await job {job_id}");
                    };
                    let outcome = await_job_watch(
                        watcher,
                        JobWatchSpec {
                            job_id: job_id.clone(),
                            deadline_ms: deadline,
                            claimed,
                        },
                        deadline,
                        now_ms(),
                    )
                    .await;
                    let at = now_ms();
                    let (back, mut more) = match outcome {
                        JobWatchOutcome::Claimed => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.job_claimed(&node, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job claim failed: {e}"))?
                        }
                        JobWatchOutcome::Settled(value) => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.job_settled(&node, value, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job settle failed: {e}"))?
                        }
                        JobWatchOutcome::Failed(error) => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.job_failed(&node, error, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job fail failed: {e}"))?
                        }
                        JobWatchOutcome::TimedOut => {
                            let at = deadline.unwrap_or_else(now_ms).max(now_ms());
                            verb(move || {
                                let mut run = run;
                                let effects = run.woke(at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job timeout failed: {e}"))?
                        }
                    };
                    run = back;
                    follow.append(&mut more);
                }
                RunEffect::SpawnDetached { spec, .. } => {
                    seams
                        .spawn_specs
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .insert(spec.spawn_key.clone(), spec.clone());
                    seams.spawn_sink.lock().await.push(spec.spawn_key.clone());
                }
                RunEffect::Complete { output } => {
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
            // bd-jhz8x.5: a verb can end the run while its batch still holds
            // parks and watches for siblings; feeding those to a terminal model
            // is a refused transition that hides the real failure.
            if run.model().is_terminal()
                && !matches!(
                    effect,
                    GraphEffect::Fail { .. }
                        | GraphEffect::Complete { .. }
                        | GraphEffect::Cancelled
                )
            {
                continue;
            }
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
                GraphEffect::WatchJob { node, job_id } => {
                    // bd-jhz8x.5: the graph model awaits a dispatched job the
                    // same way the linear model does, so the driver borrows the
                    // linear arm's shape verbatim.
                    let (deadline, claimed) = match run.model().node_state.get(&node) {
                        Some(NodeState::AwaitingJob {
                            claim_deadline_ms,
                            result_deadline_ms,
                            claimed,
                            ..
                        }) => {
                            if *claimed {
                                (*result_deadline_ms, true)
                            } else {
                                (*claim_deadline_ms, false)
                            }
                        }
                        _ => (None, false),
                    };
                    let Some(watcher) = &seams.job_watcher else {
                        anyhow::bail!("run {run_id}: no job watcher: cannot await job {job_id}");
                    };
                    let outcome = await_job_watch(
                        watcher,
                        JobWatchSpec {
                            job_id: job_id.clone(),
                            deadline_ms: deadline,
                            claimed,
                        },
                        deadline,
                        now_ms(),
                    )
                    .await;
                    let at = now_ms();
                    let (back, mut more) = match outcome {
                        JobWatchOutcome::Claimed => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.job_claimed(&node, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job claim failed: {e}"))?
                        }
                        JobWatchOutcome::Settled(value) => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.job_settled(&node, value, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job settle failed: {e}"))?
                        }
                        JobWatchOutcome::Failed(error) => {
                            let node = node.clone();
                            verb(move || {
                                let mut run = run;
                                let effects = run.job_failed(&node, error, at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job fail failed: {e}"))?
                        }
                        JobWatchOutcome::TimedOut => {
                            let at = deadline.unwrap_or_else(now_ms).max(now_ms());
                            verb(move || {
                                let mut run = run;
                                let effects = run.woke(at)?;
                                Ok::<_, HostError>((run, effects))
                            })
                            .await
                            .map_err(|e| anyhow::anyhow!("run {run_id}: job timeout failed: {e}"))?
                        }
                    };
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

#[cfg(test)]
mod dispatch_await_drive_tests {
    use super::super::durable::{JobWatchOutcome, JobWatchSpec, JobWatcher};
    use super::super::durable_host::DurableRun;
    use super::super::leaf::LeafSeams;
    use super::super::spawn::SpawnLauncher;
    use super::super::{
        ActionInvocation, ApprovalSink, AsyncEffectRunner, EffectJournalMode, SpawnSink,
        WorkflowResolver,
    };
    use super::{DriveSeams, Waiters, drive_linear};
    use crate::schema::parse_workflow;
    use serde_json::{Value, json};
    use sqlx::SqlitePool;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use toron_core::index::durable_engine::ToronEngineStore;
    use toron_core::index::durable_workflow::SyncWorkflowState;
    use toron_core::index::schema::{connect, migrate};
    use toron_core::journal::{JournalDb, ToronJournal};
    use toron_core::kernel::OurEngine;

    static N: AtomicU64 = AtomicU64::new(0);

    fn now_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    fn test_workflow(fail: bool) -> crate::schema::Workflow {
        parse_workflow(&format!(
            "name: t\ntrigger:\n  type: webhook\nsteps:\n  - name: j\n    action: dispatch_job\n    assignee: x\n    spec: {{ task: hi }}\n    await: result\n    result_timeout: 60\n{}",
            if fail { "    on_error: fail_run\n" } else { "" }
        ))
        .expect("fixture parses")
    }

    fn leaf_runner() -> AsyncEffectRunner {
        Arc::new(|_inv: &ActionInvocation| {
            Box::pin(async move { Ok(json!({"ok": true, "job_id": "job-1", "published": true})) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<Value>> + Send>,
                >
        })
    }

    fn queue_watcher(
        outcomes: Vec<JobWatchOutcome>,
    ) -> (JobWatcher, Arc<Mutex<VecDeque<JobWatchOutcome>>>) {
        let queue: Arc<Mutex<VecDeque<JobWatchOutcome>>> = Default::default();
        queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend(outcomes);
        let q = queue.clone();
        let watcher: JobWatcher = Arc::new(move |_spec: JobWatchSpec| {
            let q = q.clone();
            Box::pin(async move {
                q.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .pop_front()
                    .unwrap_or(JobWatchOutcome::TimedOut)
            })
                as std::pin::Pin<Box<dyn std::future::Future<Output = JobWatchOutcome> + Send>>
        });
        (watcher, queue)
    }

    fn drive_seams(job_watcher: JobWatcher) -> DriveSeams {
        let spawn: SpawnLauncher = Arc::new(|_spec| {
            Box::pin(async move { Err(anyhow::anyhow!("no spawn in this test")) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<Value>> + Send>,
                >
        });
        let children: crate::engine::drive::ChildDriver = Arc::new(|_, _, _| {
            Box::pin(async move { Err(anyhow::anyhow!("no children in this test")) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<Value>> + Send>,
                >
        });
        DriveSeams {
            leaf: LeafSeams {
                async_runner: Some(leaf_runner()),
                sync_runner: None,
                verify_sink: Default::default(),
                journal_mode: EffectJournalMode::Recording,
            },
            workflows: Arc::new(|_: &str| None) as WorkflowResolver,
            spawn,
            job_watcher: Some(job_watcher),
            approvals: Waiters::default(),
            approval_sink: ApprovalSink::default(),
            spawn_sink: SpawnSink::default(),
            spawn_specs: Default::default(),
            children,
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    async fn open_db(
        tag: &str,
    ) -> (
        SqlitePool,
        OurEngine<ToronEngineStore, ToronJournal>,
        SyncWorkflowState,
    ) {
        let dir = std::env::temp_dir().join(format!(
            "toron-dispatch-drive-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("test.sqlite");
        let pool = connect(path.to_str().expect("path"))
            .await
            .expect("connect");
        migrate(&pool).await.expect("migrate");
        let handle = tokio::runtime::Handle::current();
        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool.clone(), "host", 64), &handle),
        );
        let state = SyncWorkflowState::spawn(pool.clone(), &handle);
        (pool, engine, state)
    }

    /// A fake 43004 settles the await with the witness in steps.<name>.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_await_drive_completes_with_witness() {
        let (_pool, engine, state) = open_db("witness").await;
        let (watcher, _queue) = queue_watcher(vec![
            JobWatchOutcome::Claimed,
            JobWatchOutcome::Settled(json!({
                "ok": true, "job": "job-1",
                "witness": {"contract": "signed"}, "body": "done",
            })),
        ]);
        let seams = drive_seams(watcher);
        let wf = test_workflow(false);
        let (run, effects) =
            DurableRun::open(engine, state, "run-witness", wf, json!({}), now_ms()).expect("open");
        let out = drive_linear(run, effects, &seams)
            .await
            .expect("drive completes");
        assert_eq!(
            out.get("j").and_then(|s| s.get("witness")),
            Some(&json!({"contract": "signed"})),
            "witness lands in steps.j: {out}"
        );
        assert_eq!(
            out.get("j").and_then(|s| s.get("body")),
            Some(&json!("done"))
        );
    }

    /// No 43004/43006 past the result timeout is a typed failure naming
    /// the step and the bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_await_drive_no_result_times_out() {
        let (_pool, engine, state) = open_db("timeout").await;
        let (watcher, _queue) =
            queue_watcher(vec![JobWatchOutcome::Claimed, JobWatchOutcome::TimedOut]);
        let seams = drive_seams(watcher);
        let wf = test_workflow(true);
        let (run, effects) =
            DurableRun::open(engine, state, "run-timeout", wf, json!({}), now_ms()).expect("open");
        let out = drive_linear(run, effects, &seams)
            .await
            .expect("drive reaches terminal");
        let text = serde_json::to_string(&out).expect("renders");
        assert!(text.contains("result_timeout"), "names the bound: {text}");
        assert!(text.contains('j'), "names the step: {text}");
    }

    /// Restart mid-wait resumes the watch — and never republishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_await_drive_restart_resumes_watch_without_republish() {
        let dir = std::env::temp_dir().join(format!(
            "toron-dispatch-drive-restart-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("test.sqlite");
        let pool = connect(path.to_str().expect("path"))
            .await
            .expect("connect");
        migrate(&pool).await.expect("migrate");
        let handle = tokio::runtime::Handle::current();
        let wf = test_workflow(false);

        let effects = {
            let engine = OurEngine::with_journal(
                ToronEngineStore::spawn(pool.clone(), &handle),
                ToronJournal::spawn(JournalDb::new(pool.clone(), "host", 64), &handle),
            );
            let state = SyncWorkflowState::spawn(pool.clone(), &handle);
            let (mut run, _opened) = DurableRun::open(
                engine,
                state,
                "run-restart",
                wf.clone(),
                json!({}),
                now_ms(),
            )
            .expect("open");
            let effects = run
                .step_done(
                    json!({"ok": true, "job_id": "job-1", "published": true}),
                    now_ms(),
                )
                .expect("step done records the job");
            assert_eq!(effects.len(), 1, "one WatchJob: {effects:?}");
            effects
        };
        drop(effects);

        let engine = OurEngine::with_journal(
            ToronEngineStore::spawn(pool.clone(), &handle),
            ToronJournal::spawn(JournalDb::new(pool, "host", 64), &handle),
        );
        let state = SyncWorkflowState::spawn(
            connect(dir.join("test.sqlite").to_str().expect("path"))
                .await
                .expect("reconnect"),
            &handle,
        );
        let (watcher, _queue) = queue_watcher(vec![JobWatchOutcome::Settled(json!({
            "ok": true, "job": "job-1",
            "witness": {"contract": "signed"}, "body": "done",
        }))]);
        let seams = drive_seams(watcher);
        let (run, effects) =
            DurableRun::open(engine, state, "run-restart", wf, json!({}), now_ms())
                .expect("reopen");
        assert!(
            effects.iter().any(|e| matches!(
                e,
                crate::engine::durable::RunEffect::WatchJob { job_id, .. } if job_id == "job-1"
            )),
            "resume re-issues the watch: {effects:?}"
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, crate::engine::durable::RunEffect::ExecuteStep { .. })),
            "resume never republishes: {effects:?}"
        );
        let out = drive_linear(run, effects, &seams)
            .await
            .expect("drive completes after resume");
        assert_eq!(
            out.get("j").and_then(|s| s.get("witness")),
            Some(&json!({"contract": "signed"})),
            "witness lands after resume: {out}"
        );
    }
}
