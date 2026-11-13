//! Durable-execution host (moved from toron-mcp).
//!
//! [`WorkflowHost`] is the daemon-side construction site: engine + bus +
//! seams + trigger loop + sweep. toron-mcp keeps thin re-exports; the loop
//! lives here and the wire format is unchanged.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError};

use super::armor::{
    ARMOR_MAX_CONCURRENCY, ARMOR_SEND_TIMEOUT, ArmorError, SendArmor, SendFuture, SendRequest,
    passthrough,
};
use super::dispatch::{DispatchRequest, DispatchResult, EffectDispatcher, durable_route};
use super::durable::{
    GraphEffect, JobWatchOutcome, JobWatchSpec, JobWatcher, RunEffect, RunPhase as ModelPhase,
    await_job_watch, needs_graph_model, watch_job_once,
};
use super::durable_host::{DurableGraphRun, DurableRun, HostError};
use super::leaf::LeafSeams;
use super::linear_step::{GraphNodeStep, LinearStep, run_graph_node_step, run_linear_step};
use super::spawn::{SpawnLauncher, SpawnSeams, SpawnSpec};
use super::{
    ActionInvocation, ActionPartition, AsyncEffectRunner, AsyncRunJournal, Decision,
    EffectJournalMode, ParkKind, PendingEntry, PendingRegistry, ProviderErrorClass,
    ProviderOutcome, Resolution, ResolutionMismatch, ResolutionVerdict, RunInput, RunPhase,
    RunTransition, TerminalLedger, TerminalNote, TerminalRecord, WorkflowEngine, attribute_parks,
    capability_for_action_type, classify_provider_error_msg, derive_effect_key,
    failure_for_control_misdispatch, failure_for_unsupported, normalize_dispatch_result,
    terminal_digest, validate_resolution,
};
use crate::coalesce::{
    CoalesceDecision, Coalesced, DlqReason, MentionCoalescer, RequeueOutcome, RequeueSchedule,
};
use crate::dispatch::{DispatchAcquire, DispatchConfig, DispatchPermit, DispatchPool};
use crate::identity::workflow_digest;
use crate::jobs::{JobRequest, build_job_request_with_tags};
use crate::lifecycle::{LifecycleEffect, WorkflowLifecycle};
use crate::schema::{Action, Trigger, Workflow};
use crate::stamp_workflow_emitted;
use crate::trigger_filter::{
    matches_trigger, should_evaluate_trigger_event, trigger_payload, trigger_run_id,
};
use anyhow::{Context, Result};
use nostr::prelude::{Event, EventBuilder, EventId, FinalizeEvent, Keys, Kind, Tag, Timestamp};
use toron_core::bus::{Bus, SubscriptionEvent};
use toron_core::index::durable_engine::ToronEngineStore;
use toron_core::index::durable_workflow::RunState as DurableModelState;
use toron_core::index::durable_workflow::SyncWorkflowState;
use toron_core::index::workflow_effects::{
    EffectState, NewPark, NewRun, PendingKind, RunStatus, WorkflowEffectStore,
};
use toron_core::journal::{JournalDb, ToronJournal};
use toron_core::kernel::{OurEngine, RunPhase as EnginePhase};
use toron_core::mailbox::{Mailbox, SendMessage, UnwrappedMail};
use tracing::{debug, error, warn};

/// Map one mailbox send error onto a provider outcome (bd-zczp):
/// ambiguous transport outcomes become Ambiguous (I5 — a timeout is not
/// proof of absence); deterministic rejections stay ProvenFailed.
/// Project one engine-observed verify result into a [`ProviderOutcome`]
/// (bd-engine-run-path-journaling-fwlx): `{ok:true,…}` is a delivered
/// observation, an in-band `{ok:false,…}` is a PROVEN failure — the sink
/// returned, so there is no transport ambiguity to reconcile. Same rule
/// flywheel's effect journal applies to `verify`.
fn verify_outcome(observed: serde_json::Value) -> ProviderOutcome {
    match observed.get("ok").and_then(serde_json::Value::as_bool) {
        Some(true) => ProviderOutcome::Delivered(observed),
        _ => ProviderOutcome::ProvenFailed(
            observed
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("verify failed")
                .to_string(),
        ),
    }
}

fn classify_send_error(msg: String) -> ProviderOutcome {
    match classify_provider_error_msg(&msg) {
        ProviderErrorClass::Ambiguous => ProviderOutcome::Ambiguous(msg),
        ProviderErrorClass::Proven => ProviderOutcome::ProvenFailed(msg),
    }
}

/// Map one mailbox send result onto a provider outcome (bd-zczp): the
/// Delivered value carries the wire count plus the journal key so the
/// step output joins back to the effect row.
fn mail_outcome(send: Result<usize, String>, key: String) -> ProviderOutcome {
    match send {
        Ok(count) => ProviderOutcome::Delivered(serde_json::json!({
            "ok": true,
            "count": count,
            "effect_key": key,
        })),
        Err(msg) => classify_send_error(msg),
    }
}

/// Map one coalescer decision onto a provider outcome (bd-zczp / GS-062 T1):
/// Buffered and Transient-requeued deferred stay [`ProviderOutcome::Deferred`]
/// (journal [`EffectState::Accepted`] — non-Settled) until kind-43001 is on
/// the wire. FlushNow publish success / AlreadyDelivered → Delivered/Settled.
/// Exhausted → ProvenFailed + DLQ (T2); NoSigner → ProvenFailed (T3 align).
/// Async: the flush publish is awaited inline (bd-ckkf).
pub async fn mention_outcome(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    decision: CoalesceDecision,
    key: String,
    delivery: Option<&crate::DeliveryCursorStore>,
    pool: Option<&DispatchPool>,
    coalescer: Option<&std::sync::Mutex<MentionCoalescer>>,
) -> ProviderOutcome {
    match decision {
        CoalesceDecision::Buffered { .. } => ProviderOutcome::Deferred(serde_json::json!({
            "ok": true,
            "coalesced": true,
            "deferred": true,
            "buffered": true,
            "effect_key": key,
        })),
        CoalesceDecision::FlushNow(coalesced) => {
            let permit = if let Some(pool) = pool {
                match host_acquire_for_assignee(pool, &coalesced.assignee, now_ms()).await {
                    Ok(p) => Some(p),
                    Err(e) => {
                        if let Some(c) = coalescer {
                            let mut g = c.lock().unwrap_or_else(PoisonError::into_inner);
                            let assignee = coalesced.assignee.clone();
                            match g.requeue(coalesced, now_ms(), RequeueSchedule::Transient) {
                                RequeueOutcome::Queued { .. } => {
                                    return ProviderOutcome::Deferred(serde_json::json!({
                                        "ok": true,
                                        "coalesced": true,
                                        "deferred": true,
                                        "reason": format!("dispatch acquire deferred: {e}"),
                                        "effect_key": key,
                                    }));
                                }
                                RequeueOutcome::Exhausted => {
                                    g.clear_retry(&assignee);
                                    return ProviderOutcome::ProvenFailed(format!(
                                        "dispatch acquire retry budget exhausted: {e}"
                                    ));
                                }
                            }
                        }
                        return ProviderOutcome::Ambiguous(format!(
                            "dispatch acquire deferred: {e}"
                        ));
                    }
                }
            } else {
                None
            };
            let out =
                match publish_coalesced_job_with_cursor(bus, signer, &coalesced, delivery).await {
                    Ok(id) => {
                        if let Some(c) = coalescer {
                            let mut g = c.lock().unwrap_or_else(PoisonError::into_inner);
                            g.clear_retry(&coalesced.assignee);
                        }
                        ProviderOutcome::Delivered(serde_json::json!({
                            "ok": true,
                            "coalesced": false,
                            "published": true,
                            "job_id": id.to_hex(),
                            "effect_key": key,
                        }))
                    }
                    Err(CoalescePublishError::NoSigner) => {
                        if let Some(c) = coalescer {
                            let mut g = c.lock().unwrap_or_else(PoisonError::into_inner);
                            g.park_dlq(coalesced, DlqReason::NoSigner);
                        } else {
                            let _ = coalesced;
                        }
                        ProviderOutcome::ProvenFailed("no job signer configured".to_string())
                    }
                    Err(CoalescePublishError::AlreadyDelivered) => {
                        if let Some(c) = coalescer {
                            let mut g = c.lock().unwrap_or_else(PoisonError::into_inner);
                            g.clear_retry(&coalesced.assignee);
                        }
                        ProviderOutcome::Delivered(serde_json::json!({
                            "ok": true,
                            "coalesced": false,
                            "published": false,
                            "skipped": true,
                            "reason": "already delivered to session",
                            "effect_key": key,
                        }))
                    }
                    Err(CoalescePublishError::InFlight) => {
                        if let Some(c) = coalescer {
                            let mut g = c.lock().unwrap_or_else(PoisonError::into_inner);
                            let _ = g.requeue(coalesced, now_ms(), RequeueSchedule::FreshWindow);
                        } else {
                            let _ = coalesced;
                        }
                        ProviderOutcome::Ambiguous(
                            "delivery in-flight for session (concurrent reserve)".into(),
                        )
                    }
                    Err(CoalescePublishError::Transport(e)) => {
                        if let Some(c) = coalescer {
                            let mut g = c.lock().unwrap_or_else(PoisonError::into_inner);
                            let assignee = coalesced.assignee.clone();
                            match g.requeue(coalesced, now_ms(), RequeueSchedule::Transient) {
                                RequeueOutcome::Queued { .. } => {
                                    ProviderOutcome::Deferred(serde_json::json!({
                                        "ok": true,
                                        "coalesced": true,
                                        "deferred": true,
                                        "reason": format!("transport deferred: {e}"),
                                        "effect_key": key,
                                    }))
                                }
                                RequeueOutcome::Exhausted => {
                                    g.clear_retry(&assignee);
                                    drop(g);
                                    ProviderOutcome::ProvenFailed(format!(
                                        "transport retry budget exhausted: {e}"
                                    ))
                                }
                            }
                        } else {
                            ProviderOutcome::Ambiguous(e)
                        }
                    }
                };
            drop(permit);
            out
        }
    }
}

/// Map one fenced dispatch result back onto the arm's `{ok,…}` shape
/// (bd-1kr2): `Settled` returns the provider's stored outcome verbatim;
/// any other journaled state reports `ok:false` with the state named —
/// an unproven effect never reads as success.
fn fenced_outcome(key: &str, result: anyhow::Result<DispatchResult>) -> serde_json::Value {
    match result {
        Ok(res) if res.state == EffectState::Settled => res.outcome,
        Ok(res) if res.state == EffectState::Accepted => res.outcome,
        Ok(res) => serde_json::json!({
            "ok": false,
            "effect_key": key,
            "effect_state": res.state.as_str(),
            "outcome": res.outcome,
        }),
        Err(e) => serde_json::json!({
            "ok": false,
            "effect_key": key,
            "error": format!("{e:#}"),
        }),
    }
}
/// Derive the run-scoped effect identity for one invocation (bd-zczp):
/// the stable key comes from (workflow_digest, run_id, node, iteration,
/// redacted action digest) via [`derive_effect_key`]; a pre-minted
/// `RunContext.effect_key` wins when the caller set one. Returns
/// `(effect_key, action_digest)`. Secrets never enter a digest
/// unredacted: the action digest hashes the redacted canonical form.
fn scoped_effect_key(inv: &ActionInvocation) -> (String, String) {
    let rendered = serde_json::to_value(&inv.action).unwrap_or(serde_json::Value::Null);
    let (redacted, _) = crate::identity::redact_value(&rendered);
    let action_digest = crate::identity::action_digest_of(&redacted);
    let key = inv.run.effect_key.clone().unwrap_or_else(|| {
        derive_effect_key(
            &inv.run.workflow_digest,
            &inv.run.run_id,
            &inv.run.node_path,
            inv.run.iteration_key.as_deref(),
            &action_digest,
        )
    });
    (key, action_digest)
}

/// Build the fenced dispatch request for one invocation (bd-zczp): the
/// run-scoped key replaces the compat content hash, so identical
/// payloads in different runs (or nodes) no longer share a journal row.
fn scoped_request(
    inv: &ActionInvocation,
    action_digest: String,
    effect_key: String,
) -> DispatchRequest {
    let action_type = inv.action.type_name().to_string();
    let project = match &inv.action {
        Action::SendMessage { project, .. } | Action::SendDm { project, .. } => project.clone(),
        _ => None,
    };
    DispatchRequest {
        effect_key,
        run_id: inv.run.run_id.clone(),
        project,
        workflow_digest: inv.run.workflow_digest.clone(),
        node_path: inv.run.node_path.clone(),
        iteration_key: inv.run.iteration_key.clone(),
        action_type: action_type.clone(),
        action_digest,
        redacted_action: None,
        capability: capability_for_action_type(&action_type),
    }
}

/// The context-bearing production effect runner (bd-zczp, Spec 1): every
/// call crosses [`invoke`] — never the compat adapter — so effectful
/// actions arrive with run/node/effect context and journal under
/// run-scoped keys. Transport `corr` == effect key (Spec 5.4).
/// Returns runner-shaped values (`{ok:false,…}` failures, never bare
/// `Err`) so the engine's failure predicate keeps working; ambiguous
/// provider errors surface inside those values as
/// `effect_state: "unknown"` (I5), via [`invoke`]'s classification.
/// Apply ONE run-lifecycle transition to the durable journal (bd-mi9n).
///
/// This is the ONE write path for `workflow_runs`: the host's own lifecycle
/// calls AND the engine's injected [`RunJournal`] both land here, so the two
/// cannot disagree about a run and a repeat is a no-op rather than a
/// double-write.
///
/// Idempotent by construction: the row is created if absent (born `started`),
/// then CAS'd to `target` only when the current status differs. A terminal row
/// never moves (toron-core refuses the CAS), so a replayed phase cannot
/// resurrect a finished run.
pub async fn apply_run_status(
    store: &WorkflowEffectStore,
    run_id: &str,
    workflow: &str,
    workflow_digest: &str,
    target: RunStatus,
) -> anyhow::Result<()> {
    let current = match store.get_run(run_id).await? {
        Some(row) => row.status,
        None => {
            store
                .create_run(NewRun {
                    run_id: run_id.to_string(),
                    project: None,
                    workflow: workflow.to_string(),
                    workflow_digest: workflow_digest.to_string(),
                })
                .await?;
            RunStatus::Started
        }
    };
    if current != target {
        let _ = store.cas_run_status(run_id, current, target).await?;
    }
    Ok(())
}

/// The engine-facing run-lifecycle journal over this store (bd-mi9n,
/// bd-ckkf): an [`AsyncRunJournal`] — awaited inline by the engine, never
/// bridged. Best-effort: a journal failure is reported and never changes
/// the run's outcome.
///
/// The engine reports only the four phases it can prove (Started, Running,
/// Succeeded, Failed); host-level states (Suspended, Blocked, Cancelled) stay
/// the host's own calls through [`apply_run_status`].
pub fn async_run_journal(store: WorkflowEffectStore) -> AsyncRunJournal {
    Arc::new(move |t: RunTransition| {
        let target = match t.phase {
            RunPhase::Started | RunPhase::Running => RunStatus::Running,
            RunPhase::Succeeded => RunStatus::Succeeded,
            RunPhase::Failed => RunStatus::Failed,
        };
        let store = store.clone();
        Box::pin(async move {
            if let Err(e) =
                apply_run_status(&store, &t.run_id, &t.workflow, &t.workflow_digest, target).await
            {
                eprintln!(
                    "toron-mcp: run lifecycle row for {} ({:?}) not journaled: {e:#}",
                    t.run_id, t.phase
                );
            }
        })
    })
}

/// The context-bearing production effect runner (bd-zczp + bd-ckkf,
/// Spec 1 + Spec 2.2): every call crosses the boundary [`partition`] — never
/// the compat adapter — so effectful actions arrive with run/node/effect
/// context and journal under run-scoped keys. Transport `corr` == effect key
/// (Spec 5.4). Awaited inline by the engine: NO thread-hop bridge, NO
/// blocking call of any kind.
///
/// Returns runner-shaped values (`{ok:false,…}` failures, never bare
/// `Err`) so the engine's failure predicate keeps working; ambiguous
/// provider errors surface inside those values as
/// `effect_state: "unknown"` (I5), via the same normalization [`invoke`]
/// applies on the sync path.
pub fn mailbox_async_effect_runner(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    armor: Arc<SendArmor>,
) -> AsyncEffectRunner {
    mailbox_async_effect_runner_with_delivery(
        mailbox, coalescer, job_signer, dispatcher, armor, None, None,
    )
}

/// Like [`mailbox_async_effect_runner`] with a shared session
/// [`crate::DeliveryCursorStore`] so DispatchJob / cap FlushNow
/// shares peek→publish→mark with the trigger-loop timer flush (GS-062).
/// Optional [`DispatchPool`] wires captain-C acquire on the FlushNow arm
/// (same as timer flush) — production hosts pass `Some`.
pub fn mailbox_async_effect_runner_with_delivery(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    armor: Arc<SendArmor>,
    delivery: Option<Arc<crate::DeliveryCursorStore>>,
    dispatch_pool: Option<Arc<DispatchPool>>,
) -> AsyncEffectRunner {
    Arc::new(move |inv: &ActionInvocation| {
        let mailbox = mailbox.clone();
        let coalescer = coalescer.clone();
        let job_signer = job_signer.clone();
        let dispatcher = dispatcher.clone();
        let armor = armor.clone();
        let delivery = delivery.clone();
        let dispatch_pool = dispatch_pool.clone();
        let inv = inv.clone();
        Box::pin(async move {
            let outcome = match super::partition(&inv.action) {
                ActionPartition::EngineControl => {
                    Err(Box::new(failure_for_control_misdispatch(&inv.action)))
                }
                ActionPartition::Unsupported { .. } => {
                    Err(Box::new(failure_for_unsupported(&inv.action)))
                }
                ActionPartition::HostSink { .. } => normalize_dispatch_result(
                    &inv,
                    dispatch_host_sink(
                        mailbox,
                        coalescer,
                        job_signer,
                        dispatcher,
                        armor,
                        delivery,
                        dispatch_pool,
                        inv.clone(),
                    )
                    .await,
                ),
            };
            Ok(match outcome {
                Ok(o) => o.value,
                Err(f) => f.to_value(),
            })
        })
    })
}

/// Dispatch one HostSink leaf over the mailbox/coalescer/verify sinks
/// (bd-zczp): run-scoped keys, `corr == effect_key` on the wire (Spec 5.4),
/// fenced journaling when a dispatcher is attached, direct classified sends
/// otherwise. Every await is inline — the caller drives this future on the
/// engine's runtime.
#[allow(clippy::too_many_arguments)]
async fn dispatch_host_sink(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    armor: Arc<SendArmor>,
    delivery: Option<Arc<crate::DeliveryCursorStore>>,
    dispatch_pool: Option<Arc<DispatchPool>>,
    inv: ActionInvocation,
) -> anyhow::Result<serde_json::Value> {
    match inv.action.clone() {
        Action::SendMessage {
            to,
            subject,
            body,
            project,
        } => {
            let project = project.unwrap_or_default();
            let (key, digest) = scoped_effect_key(&inv);
            let mut tags = stamp_workflow_emitted(vec![]);
            tags.push(Tag::parse(["corr", key.as_str()]).expect("corr tag parses"));
            let msg = SendMessage {
                project_slug: project.clone(),
                sender_name: sender_for(&project),
                to,
                subject,
                body_md: body,
                cc: vec![],
                bcc: vec![],
                importance: None,
                ack_required: false,
                thread_id: None,
                sender_token: None,
                overseer: false,
                extra_tags: tags,
            };
            let req = scoped_request(&inv, digest, key.clone());
            send_via_dispatch(&armor, mailbox, &dispatcher, req, msg, key).await
        }
        Action::SendDm { to, body, project } => {
            let project = project.unwrap_or_default();
            let (key, digest) = scoped_effect_key(&inv);
            let mut tags = stamp_workflow_emitted(vec![]);
            tags.push(Tag::parse(["corr", key.as_str()]).expect("corr tag parses"));
            let msg = SendMessage {
                project_slug: project.clone(),
                sender_name: sender_for(&project),
                to: vec![to],
                subject: "workflow dm".to_string(),
                body_md: body,
                cc: vec![],
                bcc: vec![],
                importance: None,
                ack_required: false,
                thread_id: None,
                sender_token: None,
                overseer: false,
                extra_tags: tags,
            };
            let req = scoped_request(&inv, digest, key.clone());
            send_via_dispatch(&armor, mailbox, &dispatcher, req, msg, key).await
        }
        Action::DispatchJob { assignee, spec, .. } => {
            let mention = crate::coalesce::InboundMention {
                assignee: assignee.clone(),
                parent: String::new(),
                spec: serde_json::to_value(&spec).expect("DispatchSpec is JSON-serializable"),
            };
            let now = now_ms();
            let decision = {
                let mut c = coalescer.lock().unwrap_or_else(PoisonError::into_inner);
                c.submit(mention, now)
            };
            let (key, digest) = scoped_effect_key(&inv);
            let req = scoped_request(&inv, digest, key.clone());
            let bus = mailbox.bus().clone();
            let disp = dispatcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let Some(disp) = disp else {
                return match mention_outcome(
                    bus.as_ref(),
                    job_signer.as_ref(),
                    decision,
                    key,
                    delivery.as_deref(),
                    dispatch_pool.as_deref(),
                    Some(coalescer.as_ref()),
                )
                .await
                {
                    ProviderOutcome::Delivered(v) | ProviderOutcome::Deferred(v) => Ok(v),
                    ProviderOutcome::ProvenFailed(m) | ProviderOutcome::Ambiguous(m) => {
                        Err(anyhow::anyhow!(m))
                    }
                };
            };
            let key_out = key.clone();
            Ok(fenced_outcome(
                &key_out,
                disp.dispatch_async(&req, move |_| {
                    let bus = bus.clone();
                    let job_signer = job_signer.clone();
                    let key = key.clone();
                    let delivery = delivery.clone();
                    let pool = dispatch_pool.clone();
                    let coalescer = coalescer.clone();
                    async move {
                        mention_outcome(
                            bus.as_ref(),
                            job_signer.as_ref(),
                            decision,
                            key,
                            delivery.as_deref(),
                            pool.as_deref(),
                            Some(coalescer.as_ref()),
                        )
                        .await
                    }
                })
                .await,
            ))
        }
        Action::Verify { .. } => {
            let observed = inv.run.provider_result.clone().unwrap_or_else(
                || serde_json::json!({ "ok": false, "error": "verify sink missing" }),
            );
            let (key, digest) = scoped_effect_key(&inv);
            let req = scoped_request(&inv, digest, key.clone());
            let disp = dispatcher
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            let Some(disp) = disp else {
                return Ok(observed);
            };
            let key_out = key;
            Ok(fenced_outcome(
                &key_out,
                disp.dispatch_async(&req, move |_| async move { verify_outcome(observed) })
                    .await,
            ))
        }
        _ => Err(anyhow::anyhow!(
            "effect runner has no sink for '{}'",
            inv.action.type_name()
        )),
    }
}

/// Send one fenced mail message: journaled dispatch when a dispatcher is
/// attached, direct classified send otherwise (bd-zczp). Direct-path
/// failures return `Err` so the normalization classifies them (I5)
/// identically. Fully async — awaited inline, never bridged.
/// One tower-armored send (bd-qdup): the mailbox send as a guarded request.
/// The armor (timeout, bulkhead, breaker) bounds it; classification stays
/// with the host — this only performs.
fn armored_send_request(mailbox: Arc<Mailbox>, msg: SendMessage) -> SendRequest {
    SendRequest {
        send: Arc::new(move || {
            let msg = msg.clone();
            let mailbox = mailbox.clone();
            Box::pin(async move {
                mailbox
                    .send_message(&msg)
                    .await
                    .map(|res| res.count)
                    .map_err(|e| e.to_string())
            }) as SendFuture
        }),
    }
}

async fn send_via_dispatch(
    armor: &SendArmor,
    mailbox: Arc<Mailbox>,
    dispatcher: &Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    req: DispatchRequest,
    msg: SendMessage,
    key: String,
) -> anyhow::Result<serde_json::Value> {
    let disp = dispatcher
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .clone();
    let Some(disp) = disp else {
        return match armor.send(armored_send_request(mailbox, msg)).await {
            Ok(sent) => match sent {
                Ok(count) => Ok(serde_json::json!({ "ok": true, "count": count })),
                Err(e) => Err(anyhow::anyhow!(e)),
            },
            Err(ArmorError::BreakerOpen) => Err(anyhow::anyhow!(
                "send refused: breaker open (storm fail-fast)"
            )),
            Err(ArmorError::Ambiguous(e)) => Err(anyhow::anyhow!(e)),
        };
    };
    let key_out = key.clone();
    Ok(fenced_outcome(
        &key_out,
        disp.dispatch_async(&req, move |_| {
            let armor = armor.clone();
            let mailbox = mailbox.clone();
            let key = key.clone();
            async move {
                match armor.send(armored_send_request(mailbox, msg)).await {
                    Ok(sent) => mail_outcome(sent, key),
                    Err(ArmorError::BreakerOpen) => ProviderOutcome::ProvenFailed(
                        "send refused: breaker open (storm fail-fast)".to_string(),
                    ),
                    Err(ArmorError::Ambiguous(e)) => ProviderOutcome::Ambiguous(e),
                }
            }
        })
        .await,
    ))
}

/// Build the kind-43001 event for a coalesced mention burst (ADR-0007 F7):
/// N mentions → exactly one job. The request carries the
/// `toron:workflow-emitted` stamp (F6) so the trigger filter never
/// re-evaluates it. Returns `None` when no signer is available.
fn build_coalesced_job_event(signer: Option<&Keys>, coalesced: &Coalesced) -> Option<Event> {
    let keys = signer?;
    let parent = EventId::from_hex(coalesced.parent()).ok();
    let payload = JobRequest {
        title: format!("coalesced job for {}", coalesced.assignee),
        spec: coalesced.merged_spec(),
        assignee: Some(coalesced.assignee.clone()),
    };
    let stamped = stamp_workflow_emitted(vec![]);
    let event =
        build_job_request_with_tags(keys, parent, &payload, Timestamp::now(), stamped).ok()?;
    Some(event)
}

/// Why a coalesced burst published nothing (bd-zczp): no signer is a
/// proven never-send; a bus failure is ambiguous (the burst may have
/// landed). Providers classify them differently — never conflated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoalescePublishError {
    /// No key to sign the 43001: deterministically unsent.
    NoSigner,
    /// The bus failed mid-publish: the job may or may not exist.
    Transport(String),
    /// GS-062: DeliveryCursorStore cursor proves already delivered (all
    /// SkipAlreadyDelivered). Maps to Delivered/Settled.
    AlreadyDelivered,
    /// GS-062: keep empty solely via SkipInFlight (concurrent reserve).
    /// Must NOT Settled/Delivered — peer still publishing; Ambiguous/retry.
    InFlight,
}

/// GS-062 captain C: acquire a dispatch permit for a live host publish.
/// Parks `SleepUntilDeadline` (journal/park + [`DispatchPool::deadline_wake`]);
/// never uses the Option [`DispatchPool::try_acquire`] shim that maps fenced
/// Err→None (e2e-only).
pub async fn host_acquire_for_assignee<'a>(
    pool: &'a DispatchPool,
    assignee: &str,
    now_ms: u64,
) -> Result<DispatchPermit<'a>, String> {
    let hold_bound = pool.hold_bound_ms().max(1);
    let deadline = now_ms.saturating_add(hold_bound);
    let mut attempt_now = now_ms;
    let mut claimed_generation = pool.generation_of(assignee);
    loop {
        match pool.try_acquire_for_host(assignee, attempt_now, claimed_generation) {
            DispatchAcquire::Granted(permit) => return Ok(permit),
            DispatchAcquire::Park {
                until_ms, effects, ..
            } => {
                tracing::debug!(
                    assignee,
                    until_ms,
                    hold_bound,
                    ?effects,
                    "host DispatchAcquire::Park — sleep full park window then deadline_wake"
                );
                let wait_ms = until_ms.saturating_sub(attempt_now).max(1);
                tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                attempt_now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(until_ms);
                let _ = pool.deadline_wake(assignee, attempt_now);
                claimed_generation = pool.generation_of(assignee);
                if attempt_now >= deadline {
                    match pool.try_acquire_for_host(assignee, attempt_now, claimed_generation) {
                        DispatchAcquire::Granted(permit) => return Ok(permit),
                        other => {
                            return Err(format!(
                                "dispatch still unavailable after hold_bound park: {other:?}"
                            ));
                        }
                    }
                }
            }
            DispatchAcquire::Refused(fx) => {
                return Err(format!("dispatch refused: {fx:?}"));
            }
            DispatchAcquire::CapContention => {
                if attempt_now >= deadline {
                    return Err("dispatch cap contention".into());
                }
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                attempt_now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(attempt_now.saturating_add(50));
                claimed_generation = pool.generation_of(assignee);
            }
        }
    }
}

/// Build + publish ONE kind-43001 for a coalesced mention burst (bd-ckkf):
/// awaited inline on the caller's runtime — no scoped thread, no runtime
/// bridge of any kind. Async contexts (the trigger loop, the async runner)
/// all await this directly. Cursor-less shim over
/// [`publish_coalesced_job_with_cursor`].
pub async fn publish_coalesced_job(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    coalesced: &Coalesced,
) -> Result<EventId, CoalescePublishError> {
    publish_coalesced_job_with_cursor(bus, signer, coalesced, None).await
}

/// GS-062: session delivery-cursor filter before kind-43001 publish.
/// Session key = assignee (dispatch session), never agent pubkey.
///
/// Peeks/reserves **all** mention parent ids in the batch (not only
/// [`Coalesced::parent`]): skip the whole batch only when none remain
/// undelivered. Reserve under the lock before awaiting publish so concurrent
/// with_cursor calls cannot double-publish; release reservation on transport
/// failure.
pub async fn publish_coalesced_job_with_cursor(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    coalesced: &Coalesced,
    delivery: Option<&crate::DeliveryCursorStore>,
) -> Result<EventId, CoalescePublishError> {
    let parent_ids = coalesced.parent_ids();
    let session = coalesced.assignee.as_str();
    let mut reservation: Option<crate::ReservationGuard<'_>> = None;
    let mut filtered: Option<Coalesced> = None;
    if let Some(store) = delivery
        && !parent_ids.is_empty()
    {
        let proof_id = parent_ids.first().map(String::as_str).unwrap_or("");
        let _ = crate::agent_key_is_not_delivery_proof("unused", proof_id);
        let (keep, effects) = store.reserve_undelivered(session, parent_ids.clone());
        if keep.is_empty() {
            let any_inflight = effects
                .iter()
                .any(|e| matches!(e, crate::DeliveryEffect::SkipInFlight { .. }));
            if any_inflight {
                return Err(CoalescePublishError::InFlight);
            }
            return Err(CoalescePublishError::AlreadyDelivered);
        }
        if keep.len() != parent_ids.len() {
            filtered = Some(coalesced.retain_parents(&keep));
        }
        reservation = Some(crate::ReservationGuard::new(store, session, keep));
    }
    let to_publish = filtered.as_ref().unwrap_or(coalesced);
    let event = match build_coalesced_job_event(signer, to_publish) {
        Some(ev) => ev,
        None => {
            return Err(CoalescePublishError::NoSigner);
        }
    };
    match bus.publish(event.clone()).await {
        Ok(_) => {}
        Err(e) => {
            return Err(CoalescePublishError::Transport(format!("bus publish: {e}")));
        }
    }
    if let Some(guard) = reservation.take() {
        guard.commit();
    } else if let Some(store) = delivery
        && !parent_ids.is_empty()
    {
        store.mark_delivered(session, parent_ids);
    }
    Ok(event.id)
}

/// The sender identity for workflow-originated mail. Workflows are authored
/// for a project; the daemon uses the `workflow` agent convention until a
/// profile resolver exists (the P4 `ProfileResolver` DI seam).
fn sender_for(project: &str) -> String {
    if project.is_empty() {
        "workflow".to_string()
    } else {
        format!("workflow-{project}")
    }
}

/// GS-062 REQUEUE-EVENT-STARVATION: trigger-loop coalescer flush timer.
/// Must be ≤ default sub-window (500ms) so InFlight/Transport requeues can
/// flush without waiting for the next bus event.
pub const COALESCE_FLUSH_TICK_MS: u64 = 250;

/// Monotonic-millis clock for the coalescer (matches its injected-time API).
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The production workflow engine host: owns the engine + the workflow set
/// and runs the trigger-ingress loop over a bus.
#[derive(Clone)]
pub struct WorkflowHost {
    engine: Arc<WorkflowEngine>,
    bus: Arc<dyn Bus>,
    /// GS-062 ask-user B: definition list behind Arc so Clone (trigger-loop)
    /// shares ONE registry with soft_delete_workflow / try_start / workflows().
    /// Do NOT copy Vec on Clone — only lifecycle Arc was shared before, which
    /// left trigger clones blind to soft_delete.
    workflows: Arc<std::sync::Mutex<Vec<Workflow>>>,
    /// GS-062: soft-delete + start_admit under one lock (Buzz #7735).
    lifecycle: Arc<WorkflowLifecycle>,
    /// GS-062: session delivery-cursor dedup for mention→43001 (Buzz #7620).
    delivery: Arc<crate::DeliveryCursorStore>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    /// ADR-0007 F7: signs coalesced kind-43001 job requests. Absent → flushes
    /// are logged but not published (honest degradation).
    job_signer: Option<Keys>,
    /// bd-asx3: host-owned pending lifecycle — structured entries, terminal
    /// ledger, run states. Shared across the trigger and resolution loops.
    pending: Arc<tokio::sync::Mutex<PendingDrain>>,
    /// bd-1kr2: fenced effect dispatch (Spec 5.3) for the production
    /// runner arms. Shared with the runner (which snapshots it per call,
    /// so a late [`Self::with_effect_store`] attach also rewires sends).
    /// `None` on journal-less embeddings (direct sends).
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    /// GS-062 captain C: live host acquire path. Arc-shared on Clone so the
    /// trigger-loop host parks SleepUntilDeadline via try_acquire_for_host
    /// (Option try_acquire remains e2e-only).
    dispatch_pool: Arc<DispatchPool>,
    /// bd-host-cut-pqkg: context-free leaf seams for the durable run path.
    /// Cloned from the engine's own seams at construction, so the durable
    /// arm dispatches through the same runner the old path would have used.
    seams: LeafSeams,
    /// bd-host-cut-pqkg: approvals parked on the durable path by run id.
    /// The model has no vendor promise id, so the run id is the
    /// correlation key; `PendingDrain` never sees these entries.
    durable_parks: Arc<std::sync::Mutex<HashMap<(String, String), DurablePark>>>,
    /// bd-spawn-launcher-flip-d8mv: the spawn capability — the profile
    /// resolver that builds the spec plus the launcher that starts the child.
    /// Empty on a host that cannot spawn (cron, the bare server), which is
    /// exactly what makes the router refuse a spawn workflow before any row.
    spawn_seams: SpawnSeams,
    /// bd-host-cut-pqkg: durable runs currently driving in this process.
    /// The restart sweep skips these so a run is never double-driven (a
    /// second driver feeding `step_ok` for the same step would hit
    /// `WrongPhase` and fail the run).
    durable_active: Arc<std::sync::Mutex<HashSet<String>>>,
}

/// Find the durable park a resolution names.
///
/// Graph parks are keyed by `(run_id, node)` because one wave can park several
/// approvals at once. The linear path has one park per run and its resolution
/// carries no node, so an absent node falls back to that single entry — never to
/// one of several.
pub fn find_durable_park(
    parks: &HashMap<(String, String), DurablePark>,
    run_id: &str,
    node: Option<&str>,
) -> Option<DurablePark> {
    if let Some(node) = node {
        return parks.get(&(run_id.to_string(), node.to_string())).cloned();
    }
    let mut hits = parks
        .iter()
        .filter(|((rid, _), _)| rid == run_id)
        .map(|(_, park)| park.clone());
    let first = hits.next()?;
    hits.next().is_none().then_some(first)
}

/// One approval parked on the durable path (bd-host-cut-pqkg).
#[derive(Clone, Debug)]
pub struct DurablePark {
    /// Step or graph node awaiting the decision.
    node: String,
    /// Park generation (always 1 in this slice: the model parks one
    /// approval at a time and the entry is removed on approve).
    generation: u64,
    /// Workflow + trigger ride along for the post-approval continuation.
    workflow: Workflow,
    /// Trigger payload for the continuation drive.
    trigger: serde_json::Value,
    /// Effect identity for the continuation's steps (same helper the old
    /// path uses: `crate::identity::workflow_digest`).
    workflow_digest: String,
}

/// The host's concrete durable driver (bd-host-cut-pqkg): one SQLite pool,
/// three channel-bridged handles. Every verb is sync and must run in
/// `spawn_blocking`, never on an async worker.
type HostDurableRun = DurableRun<ToronEngineStore, ToronJournal, SyncWorkflowState>;

/// The host's concrete durable graph driver (bd-durable-graph-impl-r2x1).
type HostDurableGraphRun = DurableGraphRun<ToronEngineStore, ToronJournal, SyncWorkflowState>;

/// One spawn settlement fed back through the driver verb
/// (bd-spawn-launcher-flip-d8mv): settled, failed, or timed out.
type SpawnSettleStep =
    Box<dyn FnOnce(HostDurableRun) -> Result<(HostDurableRun, Vec<RunEffect>), HostError> + Send>;

/// A persisted durable row is one model or the other; the workflow tells them
/// apart on open, and the shape is stored with the model.
fn model_head(json: &str) -> Option<(Workflow, serde_json::Value)> {
    let model = crate::engine::durable::PersistedWorkflowModel::decode(json).ok()?;
    Some((model.workflow().clone(), model.trigger().clone()))
}

/// Build the three host handles for one pool (bd-host-cut-pqkg). Call from
/// async context: `spawn` needs the runtime handle. The handles are
/// sync-over-channels; every verb on them runs in `spawn_blocking`.
fn host_handles(pool: sqlx::SqlitePool) -> (ToronEngineStore, ToronJournal, SyncWorkflowState) {
    let handle = tokio::runtime::Handle::current();
    (
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool.clone(), "host", 64), &handle),
        SyncWorkflowState::spawn(pool, &handle),
    )
}

/// One launch attempt's outcome, bounded by the model's park deadline.
enum SpawnLaunch {
    /// The launcher returned the child's receipt.
    Settled(serde_json::Value),
    /// The launcher failed (no child, or the child failed).
    Failed(String),
    /// The deadline passed first; the fold's `Woke` owns the timeout text.
    TimedOut,
}

/// Await the host's launcher, bounded by the spawn's park deadline.
///
/// No deadline (`no_claim_timeout: 0`) means an unbounded await, the same rule
/// the replaced promise had. A deadline already in the past never launches:
/// the model would have failed the node on resume, so launching would start a
/// child for a run that has no use for it.
async fn await_spawn_launch(
    launcher: &SpawnLauncher,
    spec: &SpawnSpec,
    deadline_ms: Option<u64>,
) -> SpawnLaunch {
    let Some(deadline) = deadline_ms else {
        return match launcher(spec.clone()).await {
            Ok(value) => SpawnLaunch::Settled(value),
            Err(e) => SpawnLaunch::Failed(format!("{e:#}")),
        };
    };
    let now = durable_now_ms();
    if deadline <= now {
        return SpawnLaunch::TimedOut;
    }
    match tokio::time::timeout(
        std::time::Duration::from_millis(deadline - now),
        launcher(spec.clone()),
    )
    .await
    {
        Ok(Ok(value)) => SpawnLaunch::Settled(value),
        Ok(Err(e)) => SpawnLaunch::Failed(format!("{e:#}")),
        Err(_) => SpawnLaunch::TimedOut,
    }
}

/// Host time, unix millis, for the durable driver verbs.
fn durable_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

impl std::fmt::Debug for WorkflowHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHost")
            .field(
                "workflows",
                &self.workflows.lock().map(|g| g.len()).unwrap_or(0),
            )
            .field("engine", &self.engine)
            .field("has_job_signer", &self.job_signer.is_some())
            .field(
                "has_dispatcher",
                &self
                    .dispatcher
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .is_some(),
            )
            .field(
                "pending_len",
                &self.pending.try_lock().map(|g| g.registry_len()),
            )
            .finish()
    }
}

impl WorkflowHost {
    /// Construct the host over a mailbox: engine with the production mailbox
    /// runner, the bus from the mailbox, plus the workflow set. The operator
    /// key (when present) signs coalesced job requests (F7).
    pub fn new(mailbox: Arc<Mailbox>, workflows: Vec<Workflow>) -> Result<Self> {
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let lifecycle = Arc::new(WorkflowLifecycle::new());
        let delivery = Arc::new(crate::DeliveryCursorStore::new());
        for w in &workflows {
            let _ = lifecycle.register(w.name.clone());
        }
        let job_signer = mailbox.operator().cloned();
        let store = mailbox
            .index_pool()
            .map(|pool| WorkflowEffectStore::new(pool.clone()));
        let dispatcher = Arc::new(std::sync::Mutex::new(
            store.clone().map(EffectDispatcher::new),
        ));
        let pending = store.map_or_else(PendingDrain::new, |store| {
            PendingDrain::new().with_store(store)
        });
        let dispatch_pool = Arc::new(DispatchPool::new(DispatchConfig::default()));
        let async_effect_runner = mailbox_async_effect_runner_with_delivery(
            mailbox.clone(),
            coalescer.clone(),
            job_signer.clone(),
            dispatcher.clone(),
            Arc::new(SendArmor::new(
                passthrough(),
                ARMOR_SEND_TIMEOUT,
                ARMOR_MAX_CONCURRENCY,
            )),
            Some(delivery.clone()),
            Some(dispatch_pool.clone()),
        );
        let async_run_journal = pending
            .store()
            .map(|store| async_run_journal(store.clone()));
        let seams_runner = async_effect_runner.clone();
        let engine = WorkflowEngine::local_with_resolvers_async_effect_mode_and_journal(
            WorkflowEngine::default_resolver(),
            WorkflowEngine::default_workflow_resolver(),
            Some(async_effect_runner),
            EffectJournalMode::Enforced,
            async_run_journal,
        )
        .context("workflow engine")?;
        let engine = match pending.store() {
            Some(store) => engine.with_effect_store(store.clone()),
            None => engine,
        };
        let bus = mailbox.bus().clone();
        let seams = LeafSeams {
            async_runner: Some(seams_runner),
            sync_runner: None,
            verify_sink: engine.verify_sink().clone(),
            journal_mode: EffectJournalMode::Enforced,
        };
        let durable_parks = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let durable_active = Arc::new(std::sync::Mutex::new(HashSet::new()));
        Ok(Self {
            engine: Arc::new(engine),
            bus,
            workflows: Arc::new(std::sync::Mutex::new(workflows)),
            lifecycle,
            delivery,
            coalescer,
            job_signer,
            pending: Arc::new(tokio::sync::Mutex::new(pending)),
            dispatcher,
            dispatch_pool,
            seams,
            durable_parks,
            spawn_seams: SpawnSeams::default(),
            durable_active,
        })
    }

    /// Construct over an explicit bus (tests / embeddings without a mailbox).
    /// Call [`Self::with_job_signer`] to enable coalesced 43001 publishing.
    /// The async effect seam is injected (bd-ctj7): `None` keeps the
    /// runner-less Recording path for pure leaves.
    pub fn over_bus(
        bus: Arc<dyn Bus>,
        workflows: Vec<Workflow>,
        effect: Option<AsyncEffectRunner>,
    ) -> Result<Self> {
        let engine = WorkflowEngine::local_with_async_effect_runner(effect.clone())
            .context("workflow engine")?;
        let seams = LeafSeams {
            journal_mode: if effect.is_some() {
                EffectJournalMode::Enforced
            } else {
                EffectJournalMode::Recording
            },
            async_runner: effect,
            sync_runner: None,
            verify_sink: engine.verify_sink().clone(),
        };
        let lifecycle = Arc::new(WorkflowLifecycle::new());
        let delivery = Arc::new(crate::DeliveryCursorStore::new());
        for w in &workflows {
            let _ = lifecycle.register(w.name.clone());
        }
        Ok(Self {
            engine: Arc::new(engine),
            bus,
            workflows: Arc::new(std::sync::Mutex::new(workflows)),
            lifecycle,
            delivery,
            coalescer: Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            job_signer: None,
            pending: Arc::new(tokio::sync::Mutex::new(PendingDrain::new())),
            dispatcher: Arc::new(std::sync::Mutex::new(None)),
            dispatch_pool: Arc::new(DispatchPool::new(DispatchConfig::default())),
            seams,
            durable_parks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            spawn_seams: SpawnSeams::default(),
            durable_active: Arc::new(std::sync::Mutex::new(HashSet::new())),
        })
    }

    /// Construct over an explicit bus AND engine (bd-asx3 lifecycle tests:
    /// a remote engine is required for real promise parks).
    pub fn over_bus_with_engine(
        bus: Arc<dyn Bus>,
        engine: WorkflowEngine,
        workflows: Vec<Workflow>,
    ) -> Self {
        let lifecycle = Arc::new(WorkflowLifecycle::new());
        let delivery = Arc::new(crate::DeliveryCursorStore::new());
        for w in &workflows {
            let _ = lifecycle.register(w.name.clone());
        }
        Self {
            engine: Arc::new(engine),
            bus,
            workflows: Arc::new(std::sync::Mutex::new(workflows)),
            lifecycle,
            delivery,
            coalescer: Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            job_signer: None,
            pending: Arc::new(tokio::sync::Mutex::new(PendingDrain::new())),
            dispatcher: Arc::new(std::sync::Mutex::new(None)),
            dispatch_pool: Arc::new(DispatchPool::new(DispatchConfig::default())),
            seams: LeafSeams::default(),
            durable_parks: Arc::new(std::sync::Mutex::new(HashMap::new())),
            spawn_seams: SpawnSeams::default(),
            durable_active: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Set the key that signs coalesced kind-43001 job requests (F7). Without
    /// a signer, flushes are logged but not published.
    pub fn with_job_signer(mut self, keys: Option<Keys>) -> Self {
        self.job_signer = keys;
        self
    }

    /// Install the spawn capability (bd-spawn-launcher-flip-d8mv): the
    /// resolver that builds the spec and the launcher that starts the child.
    /// Without a launcher the host admits no `spawn` workflow at all; without
    /// a resolver every spawn would land `unknown profile`.
    pub fn with_spawn_seams(mut self, seams: SpawnSeams) -> Self {
        self.spawn_seams = seams;
        self
    }

    /// True when this host can actually perform a spawn.
    fn spawn_capable(&self) -> bool {
        self.spawn_seams.launcher.is_some()
    }

    /// Attach the durable effect journal (bd-cnr5 + bd-1kr2): run states +
    /// terminal dedupe persist in `workflow_runs` / `workflow_terminal_events`,
    /// and the shared dispatcher cell gains an `EffectDispatcher`, so the
    /// runner arms fence sends without a rebuild. Production (`new`)
    /// attaches automatically when the mailbox carries the index pool;
    /// tests attach explicitly.
    pub async fn with_effect_store(&self, store: WorkflowEffectStore) {
        self.pending.lock().await.set_store(store.clone());
        *self
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(EffectDispatcher::new(store));
    }

    /// The fenced effect dispatcher (bd-1kr2, Spec 5.3): `Some` when the
    /// host was constructed over a journal (`new` with an index pool, or
    /// after [`Self::with_effect_store`]); `None` on journal-less
    /// embeddings, where the arms send directly.
    pub fn effect_dispatcher(&self) -> Option<EffectDispatcher> {
        self.dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The engine (for tests / embedding observability).
    pub fn engine(&self) -> &WorkflowEngine {
        self.engine.as_ref()
    }

    /// The coalescer (for tests).
    pub const fn coalescer(&self) -> &Arc<std::sync::Mutex<MentionCoalescer>> {
        &self.coalescer
    }

    /// Register an additional workflow (idempotent by name).
    /// GS-062: also registers with [`WorkflowLifecycle`] under one lock.
    pub fn add_workflow(&mut self, workflow: Workflow) {
        let name = workflow.name.clone();
        {
            let defs = self
                .workflows
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if defs.iter().any(|w| w.name == name) {
                return;
            }
        }
        let _ = self.lifecycle.register(name.clone());
        let mut defs = self
            .workflows
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !defs.iter().any(|w| w.name == name) {
            defs.push(workflow);
        }
    }

    /// Soft-delete a workflow definition (Buzz #7735 / GS-062): clears
    /// executable under the lifecycle lock and drops the in-memory definition
    /// from the **shared** Arc registry so Clone'd trigger-loop hosts see it.
    pub fn soft_delete_workflow(&mut self, id: &str) -> LifecycleEffect {
        let fx = self.lifecycle.soft_delete(id);
        if matches!(
            fx,
            LifecycleEffect::SoftDeleted { .. } | LifecycleEffect::AlreadyDeleted { .. }
        ) {
            self.workflows
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .retain(|w| w.name != id);
        }
        fx
    }

    /// Lifecycle registry (tests / diagnostics).
    pub fn lifecycle(&self) -> &WorkflowLifecycle {
        self.lifecycle.as_ref()
    }

    /// GS-062 captain C: live [`DispatchPool`] (try_acquire_for_host path).
    pub fn dispatch_pool(&self) -> &DispatchPool {
        self.dispatch_pool.as_ref()
    }

    /// Snapshot of registered workflow definitions (shared Arc registry).
    pub fn workflows(&self) -> Vec<Workflow> {
        self.workflows
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// The host-owned pending lifecycle (bd-asx3).
    pub const fn pending(&self) -> &Arc<tokio::sync::Mutex<PendingDrain>> {
        &self.pending
    }

    /// Publish one approval-pending event (kind-46011) for a structured
    /// entry: run/node/effect/promise/generation plus the bounded payload.
    /// Requires a signer; without one, bail honestly instead of staging an
    /// unpublished pending.
    pub async fn publish_approval_pending(&self, entry: &PendingEntry) -> Result<EventId> {
        let signer = self
            .job_signer
            .as_ref()
            .context("no signer: cannot publish kind-46011 approval-pending")?;
        let mut tags = Vec::new();
        if let Some(project) = &entry.project {
            tags.push(Tag::parse(["t", project]).map_err(|e| anyhow::anyhow!("t tag: {e}"))?);
        }
        let mut body = serde_json::json!({
            "run_id": entry.run_id,
            "workflow": entry.workflow,
            "node": entry.node_path,
            "promise_id": entry.promise_id,
            "generation": entry.generation,
            "kind": entry.kind.as_str(),
            "question": entry.question,
            "evidence_hash": entry.evidence_hash,
            "deadline_secs": entry.deadline_secs,
        });
        if let (Some(payload), Some(obj)) = (&entry.payload, body.as_object_mut()) {
            obj.insert("payload".into(), payload.clone());
        }
        let event = EventBuilder::new(Kind::from_u16(46011), body.to_string())
            .tags(tags)
            .finalize(signer)
            .map_err(|e| anyhow::anyhow!("sign 46011: {e}"))?;
        let id = event.id;
        self.bus
            .publish(event)
            .await
            .map_err(|e| anyhow::anyhow!("publish 46011: {e}"))?;
        Ok(id)
    }

    /// Publish one spawn-await pending event (kind-43001 job request) for a
    /// structured entry, so the spawn-await park is operator-visible like an
    /// approval park. The spec carries the promise/run/node correlation.
    pub async fn publish_spawn_pending(&self, entry: &PendingEntry) -> Result<EventId> {
        let signer = self
            .job_signer
            .as_ref()
            .context("no signer: cannot publish kind-43001 spawn-pending")?;
        let payload = JobRequest {
            title: format!("spawn-await {}/{}", entry.run_id, entry.node_path),
            spec: serde_json::json!({
                "promise_id": entry.promise_id,
                "run_id": entry.run_id,
                "node": entry.node_path,
                "generation": entry.generation,
                "workflow": entry.workflow,
            }),
            assignee: None,
        };
        let mut tags = Vec::new();
        if let Some(project) = &entry.project {
            tags.push(Tag::parse(["t", project]).map_err(|e| anyhow::anyhow!("t tag: {e}"))?);
        }
        let event = build_job_request_with_tags(signer, None, &payload, Timestamp::now(), tags)
            .map_err(|e| anyhow::anyhow!("build 43001: {e}"))?;
        let id = event.id;
        self.bus
            .publish(event)
            .await
            .map_err(|e| anyhow::anyhow!("publish 43001: {e}"))?;
        Ok(id)
    }

    /// Publish one semantic terminal (kind-46012) for a run. The ledger
    /// dedupes by digest: a same-digest republish returns `Duplicate`
    /// without a bus write (no duplicate semantic terminal); a different
    /// digest for the same run returns `Conflict` and still publishes, so
    /// the conflict is evidence-visible instead of silently merged.
    pub async fn publish_terminal(
        &self,
        run_id: &str,
        workflow_digest: &str,
        status: &str,
        node: &str,
        evidence: &str,
    ) -> Result<TerminalNote> {
        let terminal_state = RunState::parse_status(status)
            .map_err(|e| anyhow::anyhow!("refusing terminal with {e}"))?;
        if !terminal_state.is_terminal() {
            anyhow::bail!("refusing non-terminal status {status:?} as a semantic terminal");
        }
        let digest = terminal_digest(run_id, workflow_digest, status, node, evidence);
        let record = TerminalRecord {
            run_id: run_id.to_string(),
            digest: digest.clone(),
            status: status.to_string(),
            node: node.to_string(),
        };
        let has_store = self.pending.lock().await.store().is_some();
        let note = if has_store {
            self.pending
                .lock()
                .await
                .note_terminal_durable(record)
                .await?
        } else {
            self.pending.lock().await.note_terminal(record)
        };
        if matches!(note, TerminalNote::Duplicate) {
            return Ok(note);
        }
        let signer = self
            .job_signer
            .as_ref()
            .context("no signer: cannot publish kind-46012 terminal")?;
        let mut outcome: serde_json::Value = serde_json::from_str(evidence)
            .ok()
            .filter(serde_json::Value::is_object)
            .unwrap_or_else(|| serde_json::json!({ "evidence": evidence }));
        if let Some(obj) = outcome.as_object_mut() {
            obj.entry("status")
                .or_insert_with(|| serde_json::json!(status));
            obj.entry("run_id")
                .or_insert_with(|| serde_json::json!(run_id));
            if status == "failed" && !node.is_empty() {
                obj.entry("failed_step")
                    .or_insert_with(|| serde_json::json!(node));
            }
        }
        let event = crate::engine::build_workflow_run_terminal(signer, run_id, &outcome)
            .map_err(|e| anyhow::anyhow!("build 46012: {e}"))?;
        self.bus
            .publish(event)
            .await
            .map_err(|e| anyhow::anyhow!("publish 46012: {e}"))?;
        if has_store {
            let store = self.pending.lock().await.store().cloned();
            if let Some(store) = store {
                let _ = store.mark_terminal_published(&digest).await;
            }
            let _ = self
                .pending
                .lock()
                .await
                .set_run_state_durable(run_id, terminal_state)
                .await;
        } else {
            self.pending
                .lock()
                .await
                .set_run_state(run_id, terminal_state);
        }
        Ok(note)
    }

    /// Parse an approval resolution event (bd-asx3 wire contract): kind-46011
    /// content JSON `{promise_id, run_id, generation, verdict, note?}` with
    /// the project in the `t` tag. Pending publications carry no `verdict`,
    /// so they never parse as resolutions. Returns `None` for anything else.
    pub fn parse_approval_resolution(event: &Event) -> Option<Resolution> {
        if event.kind.as_u16() != 46011 {
            return None;
        }
        let body: serde_json::Value = serde_json::from_str(&event.content).ok()?;
        let verdict = ResolutionVerdict::parse(body.get("verdict")?.as_str()?)?;
        let project = event.tags.iter().find_map(|t| {
            let slice = t.as_slice();
            if slice.first().map(String::as_str) == Some("t") {
                slice.get(1).cloned()
            } else {
                None
            }
        });
        Some(Resolution {
            run_id: body.get("run_id")?.as_str()?.to_string(),
            node: body
                .get("node")
                .and_then(|n| n.as_str())
                .filter(|n| !n.is_empty())
                .map(str::to_string),
            project,
            promise_id: body.get("promise_id")?.as_str()?.to_string(),
            generation: body.get("generation")?.as_u64()?,
            verdict,
            note: body
                .get("note")
                .and_then(|n| n.as_str())
                .unwrap_or_default()
                .to_string(),
        })
    }

    /// Settle one parked approval against the registry: validate identity,
    /// then resolve or reject the exact promise. Mismatches are recorded in
    /// the drain and returned as errors — they never touch a promise.
    pub async fn resolve_parked(&self, res: Resolution) -> Result<PendingEntry> {
        let park = {
            let parks = self
                .durable_parks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            find_durable_park(&parks, &res.run_id, res.node.as_deref())
        };
        if let Some(park) = park {
            return self.resolve_durable_park(res, park).await;
        }
        let entry = self
            .pending
            .lock()
            .await
            .resolve(res.clone())
            .map_err(|m| anyhow::anyhow!("resolution ignored: {m:?}"))?;
        match res.verdict {
            ResolutionVerdict::Approved => {
                self.engine
                    .resolve_approval(&entry.promise_id, &Decision::approved(res.note))
                    .await?;
            }
            ResolutionVerdict::Rejected => {
                self.engine
                    .reject_approval(&entry.promise_id, &Decision::rejected(res.note))
                    .await?;
            }
            ResolutionVerdict::SpawnCompleted => {
                anyhow::bail!("spawn verdict on an approval park: {}", entry.promise_id)
            }
        }
        if let Err(e) = self
            .pending
            .lock()
            .await
            .set_run_state_durable(&entry.run_id, RunState::Running)
            .await
        {
            warn!(run_id = %entry.run_id, "durable run state failed: {e:#}");
        }
        Ok(entry)
    }

    /// Subscribe to kind-46011 resolution events and settle parked approvals
    /// (bd-asx3 Spec 6.2): validate run/project/promise/generation before
    /// touching a promise; stale/wrong resolutions are recorded and ignored.
    pub fn spawn_resolution_loop(&self) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            let mut sub = match me
                .bus
                .subscribe_trusted(toron_core::bus::Filter::new())
                .await
            {
                Ok(sub) => sub,
                Err(e) => {
                    warn!("resolution loop: subscribe failed: {e}");
                    return;
                }
            };
            while let Some(ev) = sub.next().await {
                let SubscriptionEvent::Event(event) = ev else {
                    continue;
                };
                let Some(res) = Self::parse_approval_resolution(&event) else {
                    continue;
                };
                match me.resolve_parked(res).await {
                    Ok(entry) => debug!(
                        run_id = %entry.run_id,
                        promise_id = %entry.promise_id,
                        "approval resolution settled a parked promise"
                    ),
                    Err(e) => debug!("ignoring unresolvable approval resolution: {e:#}"),
                }
            }
        })
    }

    /// Run one workflow detached from the ingress await and drain its new
    /// parks (bd-asx3): a parked run must not hold the trigger loop, and
    /// parked work is structured + published independently of completion.
    /// Suspended runs track [`RunState::Suspended`]; only terminal task
    /// outcomes mark success or failure. A per-run drain follower keeps
    /// re-draining until the run is terminal, so parks created after the
    /// first await point are picked up without waiting for bus traffic.
    async fn spawn_run_and_drain(&self, run_id: String, workflow: Workflow, input: RunInput) {
        match self.lifecycle.try_start(&workflow.name) {
            LifecycleEffect::StartAdmitted { .. } => {}
            LifecycleEffect::NotFound { .. } => {
                let _ = self.lifecycle.register(workflow.name.clone());
            }
            other => {
                warn!(
                    run_id = %run_id,
                    workflow = %workflow.name,
                    ?other,
                    "workflow start refused by WorkflowLifecycle"
                );
                return;
            }
        }
        if let Err(e) = self
            .pending
            .lock()
            .await
            .track_run_durable(&run_id, workflow.clone())
            .await
        {
            warn!(run_id = %run_id, "durable run track failed (memory still tracked): {e:#}");
        }
        let has_store = self.pending.lock().await.store().is_some();
        if durable_route(&workflow, has_store, self.spawn_capable()).is_ok() {
            let me = self.clone();
            tokio::spawn(async move {
                me.run_durable_loop(run_id, workflow, input).await;
            });
            return;
        }
        self.spawn_legacy_run(run_id, workflow, input).await;
    }

    /// The old engine path (bd-host-cut-pqkg): `WorkflowEngine::run` plus the
    /// sink drain + follower, unchanged. Graphs, spawn, and control-flow
    /// actions stay here until their model lands.
    async fn spawn_legacy_run(&self, run_id: String, workflow: Workflow, input: RunInput) {
        let me = self.clone();
        let rid = run_id.clone();
        tokio::spawn(async move {
            match me.engine.run(&rid, input).await {
                Ok(_) => {
                    if let Err(e) = me
                        .pending
                        .lock()
                        .await
                        .set_run_state_durable(&rid, RunState::Succeeded)
                        .await
                    {
                        warn!(run_id = %rid, "durable run state failed: {e:#}");
                    }
                    debug!(run_id = rid, "workflow ran");
                }
                Err(e) => {
                    if let Err(e2) = me
                        .pending
                        .lock()
                        .await
                        .set_run_state_durable(&rid, RunState::Failed)
                        .await
                    {
                        warn!(run_id = %rid, "durable run state failed: {e2:#}");
                    }
                    warn!(run_id = rid, "workflow failed: {e}");
                }
            }
        });
        self.drain_and_publish(&run_id, &workflow).await;
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                let terminal = me.pending.lock().await.run_state(&run_id).is_terminal();
                if terminal {
                    break;
                }
                let fresh = {
                    let engine = me.engine.clone();
                    me.pending.lock().await.drain_all(&engine).await
                };
                me.publish_fresh(&fresh).await;
            }
        });
    }

    /// Drive one linear workflow on the durable host (bd-host-cut-pqkg):
    /// open the driver (resume when the model exists), then run the effect
    /// loop. Every `DurableRun` verb runs in `spawn_blocking` — the handles
    /// are sync-over-channels and must never run on an async worker.
    async fn run_durable_loop(&self, run_id: String, workflow: Workflow, input: RunInput) {
        let pool = match self.pending.lock().await.store().map(|s| s.pool().clone()) {
            Some(pool) => pool,
            None => {
                warn!(run_id = %run_id, "durable route lost its store; falling back to the old engine");
                self.spawn_legacy_run(run_id, workflow, input).await;
                return;
            }
        };
        if needs_graph_model(&workflow) {
            self.run_durable_graph_loop(run_id, workflow, input).await;
            return;
        }
        let (engine_store, journal, state) = host_handles(pool);
        let wf = workflow.clone();
        let rid = run_id.clone();
        let trig = input.trigger.clone();
        let spawn_seams = self.spawn_seams.clone();
        let opened = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(engine_store, journal);
            DurableRun::open_with_seams(
                engine,
                state,
                rid,
                wf,
                trig,
                durable_now_ms(),
                &spawn_seams,
            )
        })
        .await;
        let (run, effects) = match opened {
            Ok(Ok(pair)) => pair,
            Ok(Err(HostError::Unsupported(reason))) => {
                warn!(run_id = %run_id, "durable open refused ({reason}); falling back to the old engine");
                self.spawn_legacy_run(run_id, workflow, input).await;
                return;
            }
            Ok(Err(e)) => {
                match &e {
                    HostError::Corrupt(_) | HostError::Finished { .. } => {
                        warn!(run_id = %run_id, "durable open failed: {e}; settling the run");
                        self.settle_open_failure(&run_id, &e).await;
                    }
                    _ => {
                        warn!(run_id = %run_id, "durable open failed: {e}; leaving the model resumable for the sweep");
                    }
                }
                return;
            }
            Err(e) => {
                warn!(run_id = %run_id, "durable open task failed: {e}; leaving the model resumable for the sweep");
                return;
            }
        };
        let digest = workflow_digest(&workflow);
        self.drive_tracked(run, effects, run_id, workflow, input.trigger, digest)
            .await;
    }

    /// Drive to park-or-terminal with the run in the active set, so the
    /// restart sweep never double-drives it (bd-host-cut-pqkg).
    async fn drive_tracked(
        &self,
        run: HostDurableRun,
        effects: Vec<RunEffect>,
        run_id: String,
        workflow: Workflow,
        trigger: serde_json::Value,
        workflow_digest: String,
    ) {
        self.durable_active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(run_id.clone());
        self.drive_durable_effects(
            run,
            effects,
            run_id.clone(),
            workflow,
            trigger,
            workflow_digest,
        )
        .await;
        self.durable_active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&run_id);
    }

    /// Spawn the durable restart sweep (bd-host-cut-pqkg): one pass now,
    /// then the same path on `interval`. The old engine got resume from the
    /// server; the durable path gets it from here — without this, a daemon
    /// restart would drop every parked timer and the cut would regress.
    pub fn spawn_durable_sweep(
        &self,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            me.sweep_durable_runs().await;
            loop {
                tokio::time::sleep(interval).await;
                me.sweep_durable_runs().await;
            }
        })
    }

    /// Re-open every persisted non-terminal durable model and drive it
    /// (bd-host-cut-pqkg). Terminal models are deleted; unparsable rows are
    /// left alone (never destroy evidence); runs already driving here or
    /// parked on an approval are skipped. No store, or an unreadable table,
    /// is a quiet no-op.
    pub async fn sweep_durable_runs(&self) {
        let pool = match self.pending.lock().await.store().map(|s| s.pool().clone()) {
            Some(pool) => pool,
            None => return,
        };
        let (engine_store, journal, state) = host_handles(pool);
        let state_for_list = state.clone();
        let rows = match tokio::task::spawn_blocking(move || state_for_list.list()).await {
            Ok(Ok(rows)) => rows,
            Ok(Err(e)) => {
                warn!("durable sweep: state list failed: {e:#}");
                return;
            }
            Err(e) => {
                warn!("durable sweep: list task failed: {e}");
                return;
            }
        };
        for (run_id, json) in rows {
            // A persisted model may be a linear RunModel or a GraphModel.
            // Parsing every row as RunModel skipped all parked DAG runs as
            // "unparsable" (bd-durable-graph-resume). Classify both shapes
            // before deciding whether to delete or drive.
            let terminal = match crate::engine::durable::PersistedWorkflowModel::decode(&json) {
                Ok(model) => model.is_terminal(),
                Err(error) => {
                    warn!(
                        run_id = %run_id,
                        "durable sweep: unparsable model, leaving the row: {error}"
                    );
                    continue;
                }
            };
            if terminal {
                let state_for_delete = state.clone();
                let rid = run_id.clone();
                if let Err(e) =
                    tokio::task::spawn_blocking(move || state_for_delete.delete(&rid)).await
                {
                    warn!(run_id = %run_id, "durable sweep: delete task failed: {e}");
                }
                continue;
            }
            let live = self
                .durable_active
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains(&run_id)
                || self
                    .durable_parks
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .keys()
                    .any(|(rid, _)| rid == &run_id);
            if live {
                continue;
            }
            let me = self.clone();
            let (es, j, st) = (engine_store.clone(), journal.clone(), state.clone());
            tokio::spawn(async move {
                me.drive_swept_run(run_id, es, j, st).await;
            });
        }
    }

    /// Re-open one swept run by id and drive it (bd-host-cut-pqkg). The
    /// persisted model wins, so the passed workflow/trigger only matter for
    /// a row lost between list and open — and those come from the model we
    /// just parsed, never a guess. Workflow, trigger, and digest for the
    /// drive all come from the reopened model.
    async fn drive_swept_run(
        &self,
        run_id: String,
        engine_store: ToronEngineStore,
        journal: ToronJournal,
        state: SyncWorkflowState,
    ) {
        let listed: Option<(Workflow, serde_json::Value)> = match tokio::task::spawn_blocking({
            let state = state.clone();
            let rid = run_id.clone();
            move || state.load(&rid)
        })
        .await
        {
            Ok(Ok(Some(json))) => model_head(&json),
            _ => None,
        };
        let Some((workflow, trigger)) = listed else {
            warn!(run_id = %run_id, "durable sweep: model vanished between list and open; skipping");
            return;
        };
        let rid = run_id.clone();
        let graph = workflow.graph.is_some();
        if graph {
            let opened = tokio::task::spawn_blocking(move || {
                let engine = OurEngine::with_journal(engine_store, journal);
                DurableGraphRun::open(engine, state, rid, workflow, trigger, durable_now_ms())
            })
            .await;
            match opened {
                Ok(Ok((run, effects))) => {
                    if run.model().is_terminal() {
                        return;
                    }
                    let digest = workflow_digest(&run.model().workflow);
                    let workflow = run.model().workflow.clone();
                    let trigger = run.model().trigger.clone();
                    self.drive_graph_tracked(run, effects, run_id, workflow, trigger, digest)
                        .await;
                }
                Ok(Err(e)) => {
                    warn!(run_id = %run_id, "durable sweep: graph reopen failed: {e}");
                }
                Err(e) => {
                    warn!(run_id = %run_id, "durable sweep: graph reopen task failed: {e}");
                }
            }
            return;
        }
        let opened = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(engine_store, journal);
            DurableRun::open(engine, state, rid, workflow, trigger, durable_now_ms())
        })
        .await;
        match opened {
            Ok(Ok((run, effects))) => {
                if run.model().is_terminal() {
                    return;
                }
                let digest = workflow_digest(&run.model().workflow);
                let workflow = run.model().workflow.clone();
                let trigger = run.model().trigger.clone();
                self.drive_tracked(run, effects, run_id, workflow, trigger, digest)
                    .await;
            }
            Ok(Err(e)) => {
                warn!(run_id = %run_id, "durable sweep: reopen failed: {e}");
            }
            Err(e) => {
                warn!(run_id = %run_id, "durable sweep: reopen task failed: {e}");
            }
        }
    }

    /// The effect loop for one durable run (bd-host-cut-pqkg). `ExecuteStep`
    /// goes through `run_linear_step` with the host seams and feeds back as
    /// `step_ok` / `step_failed` (classified by `is_failure_value`, the same
    /// predicate the Context path uses); `ParkUntil` sleeps to the deadline
    /// and re-drives with `woke`; `AwaitApproval` publishes and parks;
    /// terminal effects settle the run. Returns only on park or terminal.
    async fn drive_durable_effects(
        &self,
        mut run: HostDurableRun,
        mut effects: Vec<RunEffect>,
        run_id: String,
        workflow: Workflow,
        trigger: serde_json::Value,
        workflow_digest: String,
    ) {
        let mut outputs: serde_json::Map<String, serde_json::Value> =
            run.model().outputs.clone().into_iter().collect();
        loop {
            if effects.is_empty() {
                warn!(run_id = %run_id, "durable drive ran out of effects while non-terminal; stopping");
                return;
            }
            let mut follow: Vec<RunEffect> = Vec::new();
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
                                workflow: &workflow.name,
                                workflow_digest: &workflow_digest,
                                step: &step,
                                index,
                                trigger: &trigger,
                                outputs: &mut outputs,
                                iteration,
                            },
                            &self.seams,
                        )
                        .await;
                        let then = move |mut run: HostDurableRun| {
                            run.step_done(value, durable_now_ms()).map(|eff| (run, eff))
                        };
                        let Some((back, mut eff)) = self.drive_verb(&run_id, run, then).await
                        else {
                            return;
                        };
                        run = back;
                        outputs = run.model().outputs.clone().into_iter().collect();
                        follow.append(&mut eff);
                    }
                    RunEffect::ParkUntil { until_ms, .. } => {
                        if !matches!(run.model().phase, ModelPhase::Parked { .. }) {
                            continue;
                        }
                        let now = durable_now_ms();
                        if until_ms > now {
                            tokio::time::sleep(std::time::Duration::from_millis(until_ms - now))
                                .await;
                        }
                        let Some((back, mut eff)) = self
                            .drive_verb(&run_id, run, |mut run: HostDurableRun| {
                                run.woke(durable_now_ms()).map(|eff| (run, eff))
                            })
                            .await
                        else {
                            return;
                        };
                        run = back;
                        follow.append(&mut eff);
                    }
                    RunEffect::AwaitApproval { step, message } => {
                        self.park_durable_approval(
                            &run_id,
                            &workflow,
                            &trigger,
                            &workflow_digest,
                            step,
                            message,
                            None,
                        )
                        .await;
                        return;
                    }
                    RunEffect::StartSpawn { node, spec } => {
                        if let Err(e) = self.publish_durable_spawn(&spec).await {
                            warn!(run_id = %run_id, "durable spawn publication failed (launch continues): {e:#}");
                        }
                        let Some(launcher) = self.spawn_seams.launcher.clone() else {
                            error!(
                                run_id = %run_id,
                                "INVARIANT VIOLATION: durable StartSpawn with no launcher; stopping"
                            );
                            return;
                        };
                        let deadline = match run.model().phase {
                            ModelPhase::AwaitingSpawn { deadline_ms, .. } => deadline_ms,
                            _ => None,
                        };
                        let then: SpawnSettleStep =
                            match await_spawn_launch(&launcher, &spec, deadline).await {
                                SpawnLaunch::Settled(value) => {
                                    let node = node.clone();
                                    Box::new(move |mut run| {
                                        run.spawn_settled(&node, value, durable_now_ms())
                                            .map(|eff| (run, eff))
                                    })
                                }
                                SpawnLaunch::Failed(error) => {
                                    let node = node.clone();
                                    Box::new(move |mut run| {
                                        run.spawn_failed(&node, error, durable_now_ms())
                                            .map(|eff| (run, eff))
                                    })
                                }
                                SpawnLaunch::TimedOut => {
                                    let now = durable_now_ms().max(deadline.unwrap_or(0));
                                    Box::new(move |mut run| run.woke(now).map(|eff| (run, eff)))
                                }
                            };
                        let Some((back, mut eff)) = self.drive_verb(&run_id, run, then).await
                        else {
                            return;
                        };
                        run = back;
                        outputs = run.model().outputs.clone().into_iter().collect();
                        follow.append(&mut eff);
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
                        // Watch straight from the mailbox bus: no seam, no
                        // republish, no second driver can interleave — the
                        // fold's phase guards the node either way.
                        let bus = self.bus.clone();
                        let watcher: JobWatcher = std::sync::Arc::new(move |spec: JobWatchSpec| {
                            let bus = bus.clone();
                            Box::pin(async move { watch_job_once(&bus, &spec).await })
                                as std::pin::Pin<
                                    Box<dyn std::future::Future<Output = JobWatchOutcome> + Send>,
                                >
                        });
                        let outcome = await_job_watch(
                            &watcher,
                            JobWatchSpec {
                                job_id: job_id.clone(),
                                deadline_ms: deadline,
                                claimed,
                            },
                            deadline,
                            durable_now_ms(),
                        )
                        .await;
                        let then: SpawnSettleStep = match outcome {
                            JobWatchOutcome::Claimed => {
                                let node = node.clone();
                                Box::new(move |mut run| {
                                    run.job_claimed(&node, durable_now_ms())
                                        .map(|eff| (run, eff))
                                })
                            }
                            JobWatchOutcome::Settled(value) => {
                                let node = node.clone();
                                Box::new(move |mut run| {
                                    run.job_settled(&node, value, durable_now_ms())
                                        .map(|eff| (run, eff))
                                })
                            }
                            JobWatchOutcome::Failed(error) => {
                                let node = node.clone();
                                Box::new(move |mut run| {
                                    run.job_failed(&node, error, durable_now_ms())
                                        .map(|eff| (run, eff))
                                })
                            }
                            JobWatchOutcome::TimedOut => {
                                let now = durable_now_ms().max(deadline.unwrap_or(0));
                                Box::new(move |mut run| run.woke(now).map(|eff| (run, eff)))
                            }
                        };
                        let Some((back, mut eff)) = self.drive_verb(&run_id, run, then).await
                        else {
                            return;
                        };
                        run = back;
                        outputs = run.model().outputs.clone().into_iter().collect();
                        follow.append(&mut eff);
                    }
                    RunEffect::SpawnDetached { spec, .. } => {
                        if let Err(e) = self.publish_durable_spawn(&spec).await {
                            warn!(run_id = %run_id, "durable detached spawn publication failed: {e:#}");
                        }
                        if let Some(launcher) = self.spawn_seams.launcher.clone() {
                            match launcher(spec.clone()).await {
                                Ok(_) => debug!(run_id = %run_id, "detached spawn launched"),
                                Err(e) => warn!(
                                    run_id = %run_id,
                                    "detached spawn launch failed (the run does not wait by contract): {e:#}"
                                ),
                            }
                        }
                        continue;
                    }
                    RunEffect::Complete { output } => {
                        self.settle_durable(&run_id, "succeeded", "", &output.to_string())
                            .await;
                        return;
                    }
                    RunEffect::Fail { step, output } => {
                        self.settle_durable(&run_id, "failed", &step, &output.to_string())
                            .await;
                        return;
                    }
                    RunEffect::Cancelled => {
                        self.settle_durable(&run_id, "cancelled", "", "cancelled")
                            .await;
                        return;
                    }
                    RunEffect::Unsupported { reason } => {
                        error!(
                            run_id = %run_id,
                            "INVARIANT VIOLATION: durable drive reached Unsupported ({reason}) \
                             after the model existed; stopping instead of double-running"
                        );
                        return;
                    }
                }
            }
            effects = follow;
        }
    }

    /// Publish one durable spawn job (bd-spawn-park-toron-vwu3): the WHOLE
    /// spec rides the kind-43001 `spec`, because the consumer no longer reads a
    /// vendor promise.
    pub async fn publish_durable_spawn(&self, spec: &SpawnSpec) -> Result<EventId> {
        let signer = self
            .job_signer
            .as_ref()
            .context("no signer: cannot publish kind-43001 spawn")?;
        let payload = JobRequest {
            title: format!("spawn {}/{}", spec.run_id, spec.node),
            spec: serde_json::to_value(spec)
                .map_err(|e| anyhow::anyhow!("encode spawn spec: {e}"))?,
            assignee: None,
        };
        let mut tags = Vec::new();
        if let Some(project) = &spec.project {
            tags.push(Tag::parse(["t", project]).map_err(|e| anyhow::anyhow!("t tag: {e}"))?);
        }
        let event = build_job_request_with_tags(signer, None, &payload, Timestamp::now(), tags)
            .map_err(|e| anyhow::anyhow!("build 43001: {e}"))?;
        let id = event.id;
        self.bus
            .publish(event)
            .await
            .map_err(|e| anyhow::anyhow!("publish 43001: {e}"))?;
        Ok(id)
    }

    /// Drive one graph workflow on the durable host (bd-durable-graph-impl-r2x1).
    /// Same shape as [`Self::run_durable_loop`]: open (resume when the model
    /// exists), then run the effect loop.
    async fn run_durable_graph_loop(&self, run_id: String, workflow: Workflow, input: RunInput) {
        let pool = match self.pending.lock().await.store().map(|s| s.pool().clone()) {
            Some(pool) => pool,
            None => {
                warn!(run_id = %run_id, "durable graph route lost its store; falling back to the old engine");
                self.spawn_legacy_run(run_id, workflow, input).await;
                return;
            }
        };
        let (engine_store, journal, state) = host_handles(pool);
        let wf = workflow.clone();
        let rid = run_id.clone();
        let trig = input.trigger.clone();
        let opened = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(engine_store, journal);
            DurableGraphRun::open(engine, state, rid, wf, trig, durable_now_ms())
        })
        .await;
        let (run, effects) = match opened {
            Ok(Ok(pair)) => pair,
            Ok(Err(HostError::Unsupported(reason))) => {
                warn!(run_id = %run_id, "durable graph open refused ({reason}); falling back to the old engine");
                self.spawn_legacy_run(run_id, workflow, input).await;
                return;
            }
            Ok(Err(e)) => {
                match &e {
                    HostError::Corrupt(_) | HostError::Finished { .. } => {
                        warn!(run_id = %run_id, "durable graph open failed: {e}; settling the run");
                        self.settle_open_failure(&run_id, &e).await;
                    }
                    _ => {
                        warn!(run_id = %run_id, "durable graph open failed: {e}; leaving the model resumable for the sweep");
                    }
                }
                return;
            }
            Err(e) => {
                warn!(run_id = %run_id, "durable graph open task failed: {e}; leaving the model resumable for the sweep");
                return;
            }
        };
        let digest = workflow_digest(&workflow);
        self.drive_graph_tracked(run, effects, run_id, workflow, input.trigger, digest)
            .await;
    }

    /// Drive to park-or-terminal with the graph run in the active set.
    async fn drive_graph_tracked(
        &self,
        run: HostDurableGraphRun,
        effects: Vec<GraphEffect>,
        run_id: String,
        workflow: Workflow,
        trigger: serde_json::Value,
        workflow_digest: String,
    ) {
        self.durable_active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(run_id.clone());
        self.drive_graph_effects(
            run,
            effects,
            run_id.clone(),
            workflow,
            trigger,
            workflow_digest,
        )
        .await;
        self.durable_active
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&run_id);
    }

    /// The effect loop for one durable graph run: per-node `ExecuteNode`
    /// through the single-shot executor, `ParkUntil` slept to its deadline then
    /// re-driven with `woke`, `AwaitApproval` published and parked.
    async fn drive_graph_effects(
        &self,
        mut run: HostDurableGraphRun,
        mut effects: Vec<GraphEffect>,
        run_id: String,
        workflow: Workflow,
        trigger: serde_json::Value,
        workflow_digest: String,
    ) {
        loop {
            if effects.is_empty() {
                if run.model().is_terminal() {
                    return;
                }
                debug!(run_id = %run_id, "durable graph drive stopped at a park; the sweep resumes it");
                return;
            }
            let mut follow: Vec<GraphEffect> = Vec::new();
            let mut parks: Vec<GraphEffect> = Vec::new();
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
                            error!(
                                run_id = %run_id,
                                "INVARIANT VIOLATION: graph effect names unknown node {node}"
                            );
                            return;
                        };
                        let snapshot: serde_json::Map<String, serde_json::Value> =
                            inputs.into_iter().collect();
                        let value = run_graph_node_step(
                            GraphNodeStep {
                                run_id: &run_id,
                                workflow: &run.model().workflow.name,
                                workflow_digest: &workflow_digest,
                                node_name: &node,
                                node: &node_def,
                                attempt,
                                trigger: &trigger,
                                inputs: &snapshot,
                                item: item.as_ref(),
                            },
                            &self.seams,
                        )
                        .await;
                        let then = move |mut run: HostDurableGraphRun| {
                            run.node_done(&node, value, durable_now_ms())
                                .map(|eff| (run, eff))
                        };
                        let Some((back, mut eff)) = self.drive_graph_verb(&run_id, run, then).await
                        else {
                            return;
                        };
                        run = back;
                        follow.append(&mut eff);
                    }
                    other @ (GraphEffect::ParkUntil { .. } | GraphEffect::AwaitApproval { .. }) => {
                        parks.push(other)
                    }
                    GraphEffect::ChildJoin { node, child_run_id } => {
                        debug!(
                            run_id = %run_id,
                            %node, %child_run_id,
                            "durable graph child join (no-op: the model already landed it)"
                        );
                    }
                    GraphEffect::ChildRun { node, .. } => {
                        error!(
                            run_id = %run_id,
                            %node,
                            "INVARIANT VIOLATION: durable graph opened a child run without a child resolver; stopping"
                        );
                        return;
                    }
                    GraphEffect::Complete { output } => {
                        self.settle_durable(&run_id, "succeeded", "", &output.to_string())
                            .await;
                        return;
                    }
                    GraphEffect::Fail { fail } => {
                        let bundle = fail.to_value();
                        let node = fail.node.clone();
                        self.settle_durable(&run_id, "failed", &node, &bundle.to_string())
                            .await;
                        return;
                    }
                    GraphEffect::Cancelled => {
                        self.settle_durable(&run_id, "cancelled", "", "cancelled")
                            .await;
                        return;
                    }
                    GraphEffect::Unsupported { reason } => {
                        error!(
                            run_id = %run_id,
                            "INVARIANT VIOLATION: durable graph drive reached Unsupported \
                             ({reason}) after the model existed; stopping instead of double-running"
                        );
                        return;
                    }
                }
            }
            let mut parked_for_human = false;
            for effect in parks {
                match effect {
                    GraphEffect::AwaitApproval { node, payload } => {
                        let message = payload
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("approval required")
                            .to_string();
                        self.park_durable_approval(
                            &run_id,
                            &workflow,
                            &trigger,
                            &workflow_digest,
                            node,
                            message,
                            Some(payload),
                        )
                        .await;
                        parked_for_human = true;
                    }
                    GraphEffect::ParkUntil {
                        node: _,
                        until_ms,
                        reason: _,
                    } => {
                        let now = durable_now_ms();
                        if until_ms > now {
                            tokio::time::sleep(std::time::Duration::from_millis(until_ms - now))
                                .await;
                        }
                        let Some((back, mut eff)) = self
                            .drive_graph_verb(&run_id, run, |mut run: HostDurableGraphRun| {
                                run.woke(durable_now_ms()).map(|eff| (run, eff))
                            })
                            .await
                        else {
                            return;
                        };
                        run = back;
                        follow.append(&mut eff);
                    }
                    _ => unreachable!("only parks reach this loop"),
                }
            }
            if parked_for_human {
                return;
            }
            effects = follow;
        }
    }

    /// Run one sync graph verb in `spawn_blocking` and hand the driver back.
    async fn drive_graph_verb(
        &self,
        run_id: &str,
        run: HostDurableGraphRun,
        verb: impl FnOnce(
            HostDurableGraphRun,
        ) -> Result<(HostDurableGraphRun, Vec<GraphEffect>), HostError>
        + Send
        + 'static,
    ) -> Option<(HostDurableGraphRun, Vec<GraphEffect>)> {
        match tokio::task::spawn_blocking(|| verb(run)).await {
            Ok(Ok(pair)) => Some(pair),
            Ok(Err(e)) => {
                warn!(run_id = %run_id, "durable graph verb failed: {e}; leaving the model resumable for the sweep");
                None
            }
            Err(e) => {
                warn!(run_id = %run_id, "durable graph verb task failed: {e}; leaving the model resumable for the sweep");
                None
            }
        }
    }

    /// Run one sync driver verb in `spawn_blocking` and hand the driver back
    /// (bd-host-cut-pqkg). On any failure the run is marked failed and `None`
    /// is returned, so the caller stops the loop.
    async fn drive_verb(
        &self,
        run_id: &str,
        run: HostDurableRun,
        verb: impl FnOnce(HostDurableRun) -> Result<(HostDurableRun, Vec<RunEffect>), HostError>
        + Send
        + 'static,
    ) -> Option<(HostDurableRun, Vec<RunEffect>)> {
        match tokio::task::spawn_blocking(|| verb(run)).await {
            Ok(Ok(pair)) => Some(pair),
            Ok(Err(e)) => {
                warn!(run_id = %run_id, "durable verb failed: {e}; leaving the model resumable for the sweep");
                None
            }
            Err(e) => {
                warn!(run_id = %run_id, "durable verb task failed: {e}; leaving the model resumable for the sweep");
                None
            }
        }
    }

    /// Settle an open that cannot resume (bd-host-cut-verb-fail-vdhb):
    /// a corrupt model is unreadable, and a terminal engine row already has
    /// an outcome. Never used for a transient store/engine error.
    async fn settle_open_failure(&self, run_id: &str, err: &HostError) {
        let state = match err {
            HostError::Finished { phase } => match phase {
                EnginePhase::Settled => RunState::Succeeded,
                EnginePhase::Failed => RunState::Failed,
                EnginePhase::Cancelled => RunState::Cancelled,
                EnginePhase::Running | EnginePhase::Parked | EnginePhase::Unknown => {
                    RunState::Unknown
                }
            },
            HostError::Corrupt(_) => RunState::Failed,
            _ => return,
        };
        if let Err(e) = self
            .pending
            .lock()
            .await
            .set_run_state_durable(run_id, state)
            .await
        {
            warn!(run_id = %run_id, "durable run state failed: {e:#}");
        }
    }

    /// Park one durable approval (bd-host-cut-pqkg): record the park for the
    /// resolution path, publish kind-46011 with the run id as the
    /// correlation key, and track `Suspended`. A missing signer warns and
    /// keeps the park recorded, like the old path's retained entries.
    #[allow(clippy::too_many_arguments)]
    async fn park_durable_approval(
        &self,
        run_id: &str,
        workflow: &Workflow,
        trigger: &serde_json::Value,
        workflow_digest: &str,
        step: String,
        message: String,
        payload: Option<serde_json::Value>,
    ) {
        let park = DurablePark {
            node: step.clone(),
            generation: 1,
            workflow: workflow.clone(),
            trigger: trigger.clone(),
            workflow_digest: workflow_digest.to_string(),
        };
        self.durable_parks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert((run_id.to_string(), step.clone()), park);
        let mut entry = PendingEntry::new(
            run_id,
            None,
            workflow.name.clone(),
            step,
            None,
            run_id,
            1,
            ParkKind::Approval,
        );
        entry.payload = payload;
        entry.question = Some(message.chars().take(500).collect());
        if let Err(e) = self.publish_approval_pending(&entry).await {
            warn!(run_id = %run_id, "durable approval publication failed (park retained): {e:#}");
        }
        if let Err(e) = self
            .pending
            .lock()
            .await
            .set_run_state_durable(run_id, RunState::Suspended)
            .await
        {
            warn!(run_id = %run_id, "durable run state failed: {e:#}");
        }
    }

    /// Settle one durable terminal effect (bd-host-cut-pqkg): the kind-46012
    /// semantic terminal plus the durable run state. A failed publication
    /// still settles the state, so the run never hangs silent.
    async fn settle_durable(&self, run_id: &str, status: &str, node: &str, evidence: &str) {
        match self
            .publish_terminal(run_id, "", status, node, evidence)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                warn!(run_id = %run_id, "durable terminal publication failed: {e:#}");
                let state = RunState::parse_status(status).unwrap_or(RunState::Unknown);
                if let Err(e2) = self
                    .pending
                    .lock()
                    .await
                    .set_run_state_durable(run_id, state)
                    .await
                {
                    warn!(run_id = %run_id, "durable run state failed: {e2:#}");
                }
            }
        }
    }

    /// Settle one durable-parked approval (bd-host-cut-pqkg): validate the
    /// run/generation correlation, approve through the state store, and keep
    /// driving the run. The returned entry is built from the park (promise
    /// id = run id) for the caller's logging shape.
    async fn resolve_durable_park(
        &self,
        res: Resolution,
        park: DurablePark,
    ) -> Result<PendingEntry> {
        if res.promise_id != res.run_id {
            anyhow::bail!(
                "durable resolution correlation mismatch: promise {} != run {}",
                res.promise_id,
                res.run_id
            );
        }
        if res.generation != park.generation {
            anyhow::bail!(
                "durable resolution generation mismatch: got {}, want {}",
                res.generation,
                park.generation
            );
        }
        let decision = match res.verdict {
            ResolutionVerdict::Approved => {
                serde_json::json!({"approved": true, "note": res.note.clone()})
            }
            ResolutionVerdict::Rejected => {
                serde_json::json!({"approved": false, "note": res.note.clone()})
            }
            ResolutionVerdict::SpawnCompleted => {
                anyhow::bail!("spawn verdict on a durable approval park: {}", res.run_id);
            }
        };
        self.durable_parks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&(res.run_id.clone(), park.node.clone()));
        let pool = self.pending.lock().await.store().map(|s| s.pool().clone());
        let Some(pool) = pool else {
            anyhow::bail!("durable resolve lost its store for run {}", res.run_id);
        };
        let (engine_store, journal, state) = host_handles(pool);
        let rid = res.run_id.clone();
        let workflow = park.workflow.clone();
        let trigger = park.trigger.clone();
        if workflow.graph.is_some() {
            let node = park.node.clone();
            let approved = tokio::task::spawn_blocking(move || {
                let engine = OurEngine::with_journal(engine_store, journal);
                let (mut run, _) = DurableGraphRun::open(
                    engine,
                    state,
                    rid.clone(),
                    workflow.clone(),
                    trigger,
                    durable_now_ms(),
                )?;
                run.approve(&node, decision, durable_now_ms())
                    .map(|effects| (run, effects))
            })
            .await
            .map_err(|e| anyhow::anyhow!("durable graph approve task failed: {e}"))?;
            let (run, effects) = approved
                .map_err(|e: HostError| anyhow::anyhow!("durable graph approve failed: {e}"))?;
            let entry = PendingEntry::new(
                res.run_id.clone(),
                res.project.clone(),
                park.workflow.name.clone(),
                park.node.clone(),
                None,
                res.run_id.clone(),
                park.generation,
                ParkKind::Approval,
            );
            self.drive_graph_tracked(
                run,
                effects,
                res.run_id,
                park.workflow.clone(),
                park.trigger.clone(),
                park.workflow_digest.clone(),
            )
            .await;
            return Ok(entry);
        }
        let approved = tokio::task::spawn_blocking(move || {
            let engine = OurEngine::with_journal(engine_store, journal);
            let (mut run, _) = DurableRun::open(
                engine,
                state,
                rid.clone(),
                workflow.clone(),
                trigger,
                durable_now_ms(),
            )?;
            run.approve(decision, durable_now_ms())
                .map(|effects| (run, effects))
        })
        .await
        .map_err(|e| anyhow::anyhow!("durable approve task failed: {e}"))?;
        let (run, effects) =
            approved.map_err(|e: HostError| anyhow::anyhow!("durable approve failed: {e}"))?;
        let entry = PendingEntry::new(
            res.run_id.clone(),
            res.project.clone(),
            park.workflow.name.clone(),
            park.node.clone(),
            None,
            res.run_id.clone(),
            park.generation,
            ParkKind::Approval,
        );
        self.drive_tracked(
            run,
            effects,
            res.run_id,
            park.workflow.clone(),
            park.trigger.clone(),
            park.workflow_digest.clone(),
        )
        .await;
        Ok(entry)
    }

    /// Drain one run's fresh parks and publish their pending events.
    async fn drain_and_publish(&self, run_id: &str, workflow: &Workflow) {
        for kind in [ParkKind::Approval, ParkKind::Spawn] {
            let ctx = ParkCtx {
                run_id: run_id.to_string(),
                workflow: workflow.name.clone(),
                project: None,
                kind,
                candidates: park_candidates(workflow, kind),
            };
            let fresh = self.pending.lock().await.drain(&self.engine, &ctx).await;
            self.publish_fresh(&fresh).await;
        }
    }

    /// Re-drain every tracked non-terminal run (per loop tick, so parks are
    /// picked up independently of any one run's await) and publish fresh
    /// pending events.
    async fn drain_all_known(&self) {
        let fresh = {
            let engine = self.engine.clone();
            self.pending.lock().await.drain_all(&engine).await
        };
        self.publish_fresh(&fresh).await;
    }

    /// Publish pending events for freshly drained entries. Failures warn;
    /// the entries stay in the registry for the next attempt.
    async fn publish_fresh(&self, fresh: &[PendingEntry]) {
        for entry in fresh {
            let published = match entry.kind {
                ParkKind::Approval => self.publish_approval_pending(entry).await.map(|_| ()),
                ParkKind::Spawn => self.publish_spawn_pending(entry).await.map(|_| ()),
            };
            if let Err(e) = published {
                warn!(
                    run_id = %entry.run_id,
                    promise_id = %entry.promise_id,
                    "pending publication failed (entry retained): {e:#}"
                );
            }
        }
    }

    /// Spawn the trigger-ingress loop: subscribe to the bus, apply
    /// [`should_evaluate_trigger_event`] (loop prevention), match workflow
    /// triggers, evaluate workflow guards, and invoke the engine. Also ticks
    /// the coalescer so buffered @mention bursts flush to one job each.
    ///
    /// Returns the background task handle. The loop exits when the
    /// subscription closes.
    pub fn spawn_trigger_loop(&self) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            if let Err(e) = me.run_trigger_loop().await {
                warn!("workflow trigger loop exited: {e}");
            }
            me.persist_delivery_cursors().await;
        })
    }

    /// ADR-0007 R3: spawn a **post-unwrap** message_posted loop fed by
    /// [`toron_core::mailbox::TriggerNotifySink`] (not raw kind-1059).
    ///
    /// F1 e2e MUST send via gift-wrap so unwrap projects → notify → here.
    pub fn spawn_mail_trigger_loop(
        &self,
        mut rx: tokio::sync::mpsc::UnboundedReceiver<UnwrappedMail>,
    ) -> tokio::task::JoinHandle<()> {
        let me = self.clone();
        tokio::spawn(async move {
            while let Some(mail) = rx.recv().await {
                let stamped = mail.rumor.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("t")
                        && t.as_slice().get(1).map(String::as_str)
                            == Some(crate::trigger_filter::WORKFLOW_EMITTED_TAG)
                });
                if stamped {
                    continue;
                }
                let kind = mail.rumor.kind.as_u16();
                let defs = me
                    .workflows
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .clone();
                for workflow in &defs {
                    let Some(trigger) = &workflow.trigger else {
                        continue;
                    };
                    let ok = match trigger {
                        Trigger::MessagePosted { .. } => kind == 14 || kind == 15,
                        _ => false,
                    };
                    if !ok {
                        continue;
                    }
                    let payload = serde_json::json!({
                        "id": mail.rumor_id.to_hex(),
                        "sender": mail.sender.to_hex(),
                        "content": mail.rumor.content,
                        "kind": kind,
                        "created_at": mail.created_at.as_secs(),
                    });
                    if let Some(guard) = &workflow.guard {
                        let mut vars = std::collections::HashMap::new();
                        if let Some(obj) = payload.as_object() {
                            for (k, v) in obj {
                                let name = format!("trigger_{k}");
                                let text = match v {
                                    serde_json::Value::String(s) => s.clone(),
                                    other => other.to_string(),
                                };
                                vars.insert(name, evalexpr::Value::from(text));
                            }
                        }
                        let outcome =
                            crate::guard::evaluate_guard_default(Some(guard), &vars).await;
                        if !matches!(outcome, crate::guard::GuardOutcome::Bool(true)) {
                            continue;
                        }
                    }
                    let run_id = format!("wf-mail-{}-{}", workflow.name, mail.rumor_id.to_hex());
                    let input = RunInput {
                        workflow: workflow.clone(),
                        trigger: payload,
                        run_id: None,
                    };
                    me.spawn_run_and_drain(run_id, workflow.clone(), input)
                        .await;
                }
                me.drain_all_known().await;
            }
            me.persist_delivery_cursors().await;
        })
    }

    /// Tick the coalescer and publish (or requeue) each flushed burst.
    /// Shared by the bus-event arm and the GS-062 starvation flush timer.
    /// Idle bound for cursor rows (bd-aqvg): whole sessions older than 90d prune.
    /// A pruned session re-delivers on next sighting — a bounded duplicate,
    /// chosen over unbounded table growth.
    const CURSOR_IDLE_PRUNE_US: i64 = 90 * 24 * 3_600 * 1_000_000;

    /// Load persisted cursors once (bd-aqvg). First caller wins the boot-load
    /// race; losers skip. Merge never shrinks, so a raced double-load only
    /// re-merges. Journal-less hosts skip silently.
    async fn ensure_cursors_loaded(&self) {
        if !self.delivery.take_boot_load() {
            return;
        }
        let disp = self
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(d) = disp else { return };
        match d.store().load_delivery_cursors().await {
            Ok(rows) => {
                let mut snap = std::collections::BTreeMap::new();
                for (session, json, cursor) in rows {
                    match serde_json::from_str::<Vec<String>>(&json) {
                        Ok(ids) => {
                            snap.insert(
                                session,
                                crate::DeliveryCursor {
                                    delivered: ids.into_iter().collect(),
                                    in_flight: std::collections::BTreeSet::new(),
                                    cursor: cursor.max(0) as u64,
                                },
                            );
                        }
                        Err(e) => warn!("delivery cursor row corrupt, skipping: {e}"),
                    }
                }
                let n: usize = snap.values().map(|c| c.delivered.len()).sum();
                self.delivery.restore(snap);
                debug!("delivery cursors boot-loaded: {n} ids");
            }
            Err(e) => warn!("delivery cursor boot load failed: {e:#}"),
        }
    }

    /// Persist every host's dirty cursor marks (bd-slkn): the clean-stop
    /// counterpart to the flush-tick persist. Warn-only per host; one host's
    /// failure never blocks the others' save.
    pub async fn persist_hosts_cursors(hosts: &[Self]) {
        for h in hosts {
            h.persist_delivery_cursors().await;
        }
    }

    /// Save cursor snapshots (bd-aqvg): upsert every session plus idle prune.
    /// Called by the flush tick when dirty; warn-only — persistence must
    /// never fail delivery. Crash window = marks since the last saved tick.
    async fn persist_delivery_cursors(&self) {
        if !self.delivery.take_dirty() {
            return;
        }
        let disp = self
            .dispatcher
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(d) = disp else { return };
        let snap = self.delivery.snapshot();
        if snap.is_empty() {
            return;
        }
        let store = d.store();
        for (session, cur) in &snap {
            let json = serde_json::to_string(&cur.delivered).unwrap_or_else(|_| "[]".into());
            if let Err(e) = store
                .save_delivery_cursor(session, &json, cur.cursor as i64)
                .await
            {
                warn!("delivery cursor save failed: {e:#}");
                return;
            }
        }
        let cutoff = toron_core::index::workflow_effects::now_us() - Self::CURSOR_IDLE_PRUNE_US;
        if let Err(e) = store.prune_delivery_cursors(cutoff).await {
            warn!("delivery cursor prune failed: {e:#}");
        }
    }

    async fn flush_coalesced_bursts(&self) {
        self.ensure_cursors_loaded().await;
        let flushed = {
            let mut c = self
                .coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            c.tick(now_ms())
        };
        if !flushed.is_empty() {
            debug!("coalescer flushed {} assignee(s)", flushed.len());
        }
        for coalesced in flushed {
            let tick_now = now_ms();
            let permit = match host_acquire_for_assignee(
                self.dispatch_pool.as_ref(),
                &coalesced.assignee,
                tick_now,
            )
            .await
            {
                Ok(p) => p,
                Err(e) => {
                    let assignee = coalesced.assignee.clone();
                    let mut c = self
                        .coalescer
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    match c.requeue(coalesced, now_ms(), RequeueSchedule::Transient) {
                        RequeueOutcome::Queued { attempt, delay_ms } => warn!(
                            assignee = %assignee,
                            attempt,
                            delay_ms,
                            "coalesced burst deferred (dispatch acquire): {e} — requeue"
                        ),
                        RequeueOutcome::Exhausted => {
                            c.clear_retry(&assignee);
                            warn!(
                                assignee = %assignee,
                                "coalesced burst DLQ (dispatch acquire retry budget exhausted): {e}"
                            );
                        }
                    }
                    drop(c);
                    continue;
                }
            };
            match publish_coalesced_job_with_cursor(
                self.bus.as_ref(),
                self.job_signer.as_ref(),
                &coalesced,
                Some(self.delivery.as_ref()),
            )
            .await
            {
                Ok(id) => {
                    let mut c = self
                        .coalescer
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    c.clear_retry(&coalesced.assignee);
                    drop(c);
                    debug!(
                        assignee = %coalesced.assignee, %id,
                        "kind-43001 published for coalesced burst"
                    );
                }
                Err(CoalescePublishError::AlreadyDelivered) => {
                    let mut c = self
                        .coalescer
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    c.clear_retry(&coalesced.assignee);
                    drop(c);
                    debug!(
                        assignee = %coalesced.assignee,
                        "kind-43001 skipped (already delivered to session)"
                    );
                }
                Err(CoalescePublishError::InFlight) => {
                    warn!(
                        assignee = %coalesced.assignee,
                        "kind-43001 in-flight (concurrent reserve) — requeue"
                    );
                    let mut c = self
                        .coalescer
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    let _ = c.requeue(coalesced, now_ms(), RequeueSchedule::FreshWindow);
                    drop(c);
                }
                Err(CoalescePublishError::NoSigner) => {
                    let assignee = coalesced.assignee.clone();
                    let mut c = self
                        .coalescer
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    c.park_dlq(coalesced, DlqReason::NoSigner);
                    drop(c);
                    warn!(
                        assignee = %assignee,
                        "coalesced burst NOT published (no job signer) — parked DLQ"
                    );
                }
                Err(CoalescePublishError::Transport(e)) => {
                    let assignee = coalesced.assignee.clone();
                    let mut c = self
                        .coalescer
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner);
                    match c.requeue(coalesced, now_ms(), RequeueSchedule::Transient) {
                        RequeueOutcome::Queued { attempt, delay_ms } => warn!(
                            assignee = %assignee,
                            attempt,
                            delay_ms,
                            "coalesced burst publish transport failed: {e} — requeue"
                        ),
                        RequeueOutcome::Exhausted => {
                            c.clear_retry(&assignee);
                            warn!(
                                assignee = %assignee,
                                "coalesced burst DLQ (transport retry budget exhausted): {e}"
                            );
                        }
                    }
                    drop(c);
                }
            }
            drop(permit);
        }
        self.persist_delivery_cursors().await;
    }

    /// Drive the trigger loop to completion (until the subscription closes).
    pub async fn run_trigger_loop(&self) -> Result<()> {
        let mut sub = self
            .bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .map_err(|e| anyhow::anyhow!("bus subscribe_trusted: {e}"))?;
        let mut flush_tick =
            tokio::time::interval(std::time::Duration::from_millis(COALESCE_FLUSH_TICK_MS));
        flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        flush_tick.tick().await;
        loop {
            let event = tokio::select! {
                ev = sub.next() => {
                    let Some(ev) = ev else { break; };
                    let SubscriptionEvent::Event(event) = ev else {
                        continue;
                    };
                    self.flush_coalesced_bursts().await;
                    event
                }
                _ = flush_tick.tick() => {
                    self.flush_coalesced_bursts().await;
                    continue;
                }
            };
            if !should_evaluate_trigger_event(&event) {
                debug!(kind = event.kind.as_u16(), "event filtered at ingress");
                continue;
            }
            debug!(kind = event.kind.as_u16(), "event passes ingress filter");
            let defs = self
                .workflows
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone();
            for workflow in &defs {
                let Some(trigger) = &workflow.trigger else {
                    continue;
                };
                if !matches_trigger(trigger, &event) {
                    continue;
                }
                if let Some(guard) = &workflow.guard {
                    let payload = trigger_payload(&event);
                    let mut vars = std::collections::HashMap::new();
                    if let Some(obj) = payload.as_object() {
                        for (k, v) in obj {
                            let name = format!("trigger_{k}");
                            let text = match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            };
                            vars.insert(name, evalexpr::Value::from(text));
                        }
                    }
                    let outcome = crate::guard::evaluate_guard_default(Some(guard), &vars).await;
                    debug!(
                        outcome = ?outcome,
                        "workflow {} guard evaluated",
                        workflow.name
                    );
                    if !matches!(outcome, crate::guard::GuardOutcome::Bool(true)) {
                        debug!("workflow {} guard not satisfied", workflow.name);
                        continue;
                    }
                }
                let run_id = trigger_run_id(&workflow.name, &event);
                let input = RunInput {
                    workflow: workflow.clone(),
                    trigger: trigger_payload(&event),
                    run_id: None,
                };
                self.spawn_run_and_drain(run_id, workflow.clone(), input)
                    .await;
            }
            self.drain_all_known().await;
        }
        Ok(())
    }
}

/// Park observation context: the run whose new sink ids are being drained,
/// with the schema candidate nodes that may have parked, in order.
#[derive(Debug, Clone)]
pub struct ParkCtx {
    pub run_id: String,
    pub workflow: String,
    pub project: Option<String>,
    pub kind: ParkKind,
    pub candidates: Vec<String>,
}

/// Scan a workflow schema for parked-action node names of one kind, in
/// authoring order: linear steps first (named, or `step-{i}` fallback), then
/// graph nodes by key (BTreeMap order is deterministic).
pub fn park_candidates(workflow: &Workflow, kind: ParkKind) -> Vec<String> {
    const fn is_park(action: &Action, kind: ParkKind) -> bool {
        match (action, kind) {
            (Action::RequestApproval { .. }, ParkKind::Approval) => true,
            (Action::Spawn { await_, .. }, ParkKind::Spawn) => *await_,
            _ => false,
        }
    }
    let mut out = Vec::new();
    for (i, step) in workflow.steps.iter().enumerate() {
        if is_park(&step.action, kind) {
            out.push(step.name.clone().unwrap_or_else(|| format!("step-{i}")));
        }
    }
    if let Some(graph) = &workflow.graph {
        for (name, node) in &graph.nodes {
            if is_park(&node.action, kind) {
                out.push(name.clone());
            }
        }
    }
    out
}

/// Host-tracked run status. `Suspended` (parked on a promise) is distinct
/// from `Succeeded`/`Failed` — a parked run is never logged as failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunState {
    Running,
    Suspended,
    ReconcilePending,
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
    Unknown,
}

impl RunState {
    /// True once the run reached any terminal state.
    pub const fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Running | Self::Suspended | Self::ReconcilePending
        )
    }

    /// Canonical wire string for this state.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::ReconcilePending => "reconcile_pending",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Blocked => "blocked",
            Self::Cancelled => "cancelled",
            Self::Unknown => "unknown",
        }
    }

    /// Exhaustive status-string parse (bd-cnr5): every known run status
    /// maps explicitly; unrecognized strings and typos error loud instead
    /// of silently becoming `Failed`.
    pub fn parse_status(s: &str) -> Result<Self, String> {
        match s {
            "running" | "started" => Ok(Self::Running),
            "suspended" => Ok(Self::Suspended),
            "reconcile_pending" => Ok(Self::ReconcilePending),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "blocked" => Ok(Self::Blocked),
            "cancelled" => Ok(Self::Cancelled),
            "unknown" => Ok(Self::Unknown),
            _ => Err(format!("unknown run status {s:?}")),
        }
    }

    /// Map onto the durable journal status.
    pub const fn to_run_status(self) -> toron_core::index::workflow_effects::RunStatus {
        use toron_core::index::workflow_effects::RunStatus as S;
        match self {
            Self::Running => S::Running,
            Self::Suspended => S::Suspended,
            Self::ReconcilePending => S::ReconcilePending,
            Self::Succeeded => S::Succeeded,
            Self::Failed => S::Failed,
            Self::Blocked => S::Blocked,
            Self::Cancelled => S::Cancelled,
            Self::Unknown => S::Unknown,
        }
    }

    /// Map from the durable journal status (`Started` has no host
    /// counterpart yet — it reads as `Running`).
    pub const fn from_run_status(s: toron_core::index::workflow_effects::RunStatus) -> Self {
        use toron_core::index::workflow_effects::RunStatus as S;
        match s {
            S::Started | S::Running => Self::Running,
            S::Suspended => Self::Suspended,
            S::ReconcilePending => Self::ReconcilePending,
            S::Succeeded => Self::Succeeded,
            S::Failed => Self::Failed,
            S::Blocked => Self::Blocked,
            S::Cancelled => Self::Cancelled,
            S::Unknown => Self::Unknown,
        }
    }
}

/// Host-owned pending drain: registry + terminal ledger + per-(run, node)
/// generation counters + known promise ids + run states + recorded
/// mismatches. One drain serves every tracked run, independently of any
/// single `engine.run` await; re-draining the same live sinks after a
/// restart reconstructs the identical registry (deterministically
/// replayable — no durable row required at Wave 1).
///
/// bd-cnr5: an optional [`WorkflowEffectStore`] backs the run-state map
/// and the terminal dedupe through the durable `workflow_runs` /
/// `workflow_terminal_events` tables (Spec 5.1/I12). Memory stays the
/// fast path; the store is authoritative across restarts.
#[derive(Debug, Default)]
pub struct PendingDrain {
    registry: PendingRegistry,
    ledger: TerminalLedger,
    generations: std::collections::HashMap<(String, String), u64>,
    known: std::collections::HashSet<String>,
    runs: std::collections::HashMap<String, RunState>,
    run_workflows: std::collections::HashMap<String, Workflow>,
    ignored: Vec<(Resolution, ResolutionMismatch)>,
    store: Option<WorkflowEffectStore>,
}

impl PendingDrain {
    /// Empty drain.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach the durable journal (bd-cnr5). Memory stays the fast path;
    /// the store is authoritative across restarts.
    pub fn with_store(mut self, store: WorkflowEffectStore) -> Self {
        self.store = Some(store);
        self
    }

    /// Attach the durable journal in place.
    pub fn set_store(&mut self, store: WorkflowEffectStore) {
        self.store = Some(store);
    }

    /// The attached durable journal, if any.
    pub const fn store(&self) -> Option<&WorkflowEffectStore> {
        self.store.as_ref()
    }

    /// Track a run's workflow for later re-drains (called when the run starts).
    pub fn track_run(&mut self, run_id: &str, workflow: Workflow) {
        self.run_workflows.insert(run_id.to_string(), workflow);
        self.runs.insert(run_id.to_string(), RunState::Running);
    }

    /// Track a run durably (bd-cnr5): memory plus an idempotent
    /// `workflow_runs` row, transitioned to `Running`.
    ///
    /// bd-mi9n: the durable write goes through [`apply_run_status`] — the SAME
    /// path the engine's injected [`RunJournal`] uses — so the host and the
    /// engine agree on the row and a repeat is a no-op, never a double-write.
    pub async fn track_run_durable(
        &mut self,
        run_id: &str,
        workflow: Workflow,
    ) -> anyhow::Result<()> {
        let workflow_name = workflow.name.clone();
        self.track_run(run_id, workflow);
        if let Some(store) = self.store.clone() {
            apply_run_status(
                &store,
                run_id,
                &workflow_name,
                "",
                toron_core::index::workflow_effects::RunStatus::Running,
            )
            .await?;
        }
        Ok(())
    }

    /// Current tracked state (`Running` when never observed).
    pub fn run_state(&self, run_id: &str) -> RunState {
        self.runs.get(run_id).copied().unwrap_or(RunState::Running)
    }

    /// Current tracked state with durable fallback (bd-cnr5): memory
    /// first, then the `workflow_runs` row (restart recovery).
    pub async fn run_state_durable(&self, run_id: &str) -> RunState {
        if let Some(state) = self.runs.get(run_id).copied()
            && (state != RunState::Running || self.store.is_none())
        {
            return state;
        }
        if let Some(store) = self.store.as_ref()
            && let Ok(Some(row)) = store.get_run(run_id).await
        {
            return RunState::from_run_status(row.status);
        }
        self.run_state(run_id)
    }

    /// Set the tracked state.
    pub fn set_run_state(&mut self, run_id: &str, state: RunState) {
        self.runs.insert(run_id.to_string(), state);
    }

    /// Set the tracked state durably (bd-cnr5): memory plus a
    /// compare-and-swap on the `workflow_runs` row. Terminal rows never
    /// move; a refused CAS leaves the journal untouched (memory still
    /// updates — the journal is authoritative, never corrupted).
    ///
    /// bd-mi9n: routes through [`apply_run_status`] — the SAME path the
    /// engine's injected [`RunJournal`] uses.
    pub async fn set_run_state_durable(
        &mut self,
        run_id: &str,
        state: RunState,
    ) -> anyhow::Result<()> {
        self.set_run_state(run_id, state);
        if let Some(store) = self.store.clone() {
            let workflow = self
                .run_workflows
                .get(run_id)
                .map(|w| w.name.clone())
                .unwrap_or_else(|| "unknown".to_string());
            apply_run_status(&store, run_id, &workflow, "", state.to_run_status()).await?;
        }
        Ok(())
    }

    /// Record a terminal publication durably (bd-cnr5): the journal's
    /// `note_terminal_pub` is authoritative (same digest → Duplicate,
    /// rival digest → Conflict); memory mirrors the outcome for the fast
    /// path. Returns the durable outcome when a store is attached.
    pub async fn note_terminal_durable(
        &mut self,
        record: TerminalRecord,
    ) -> anyhow::Result<TerminalNote> {
        let Some(store) = self.store.clone() else {
            return Ok(self.ledger.note(record));
        };
        use toron_core::index::workflow_effects::{TerminalOutcome, TerminalPub};
        let outcome = store
            .note_terminal_pub(TerminalPub {
                terminal_digest: record.digest.clone(),
                run_id: record.run_id.clone(),
                status: record.status.clone(),
                node_path: Some(record.node.clone()),
                evidence: None,
            })
            .await?;
        let _ = self.ledger.note(record);
        Ok(match outcome {
            TerminalOutcome::New => TerminalNote::New,
            TerminalOutcome::Duplicate => TerminalNote::Duplicate,
            TerminalOutcome::Conflict { existing_digest } => {
                TerminalNote::Conflict { existing_digest }
            }
        })
    }

    /// Drain one run's new sink ids of the context kind into structured
    /// entries. Unknown future ids stay unmarked (retried next drain);
    /// count-mismatched ids stay unmarked too — never guessed.
    pub async fn drain(&mut self, engine: &WorkflowEngine, ctx: &ParkCtx) -> Vec<PendingEntry> {
        let ids: Vec<String> = match ctx.kind {
            ParkKind::Approval => engine.approval_sink().lock().await.clone(),
            ParkKind::Spawn => engine.spawn_sink().lock().await.clone(),
        };
        let fresh: Vec<String> = ids
            .into_iter()
            .filter(|id| !self.known.contains(id))
            .collect();
        if fresh.is_empty() {
            return Vec::new();
        }
        self.runs.insert(ctx.run_id.clone(), RunState::Suspended);
        let attributed = attribute_parks(&ctx.candidates, &fresh);
        if attributed.is_empty() {
            warn!(
                run_id = %ctx.run_id,
                candidates = ctx.candidates.len(),
                fresh = fresh.len(),
                "park attribution refused: candidate/promise counts mismatch, retrying next drain"
            );
            return Vec::new();
        }
        let mut out = Vec::new();
        for (node, id) in attributed {
            let generation = self
                .generations
                .entry((ctx.run_id.clone(), node.clone()))
                .or_insert(0);
            *generation += 1;
            let entry = PendingEntry::new(
                ctx.run_id.clone(),
                ctx.project.clone(),
                ctx.workflow.clone(),
                node.clone(),
                None,
                id.clone(),
                *generation,
                ctx.kind,
            );
            let observed = self.registry.observe(entry.clone());
            if let Some(store) = self.store.clone() {
                let kind = match ctx.kind {
                    ParkKind::Approval => PendingKind::Approval,
                    ParkKind::Spawn => PendingKind::Spawn,
                };
                let effect_key = format!("park:{}:{}:{}", ctx.run_id, node, id);
                let digest = store
                    .get_run(&ctx.run_id)
                    .await
                    .ok()
                    .flatten()
                    .map(|r| r.workflow_digest);
                let write = match digest {
                    Some(workflow_digest) => store
                        .park_effect(NewPark {
                            effect_key,
                            run_id: ctx.run_id.clone(),
                            promise_id: id.clone(),
                            park_generation: *generation as i64,
                            project: ctx.project.clone(),
                            workflow: ctx.workflow.clone(),
                            workflow_digest,
                            node_path: node.clone(),
                            kind,
                            until_ms: entry.deadline_secs.map(|s| s.saturating_mul(1000)),
                            reason: entry.question.clone(),
                        })
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
                    None => Err(format!("no run row for {}", ctx.run_id)),
                };
                match write {
                    Ok(()) => {
                        self.known.insert(id);
                    }
                    Err(e) => {
                        warn!(run_id = %ctx.run_id, promise_id = %id, "park persist failed, retrying next drain: {e}");
                    }
                }
            } else {
                self.known.insert(id);
            }
            if observed {
                out.push(entry);
            }
        }
        out
    }

    /// Re-drain every tracked non-terminal run (the loop calls this per
    /// tick, so parks are picked up independently of any one run's await).
    pub async fn drain_all(&mut self, engine: &WorkflowEngine) -> Vec<PendingEntry> {
        let live: Vec<(String, Workflow)> = self
            .run_workflows
            .iter()
            .filter(|(run_id, _)| !self.run_state(run_id).is_terminal())
            .map(|(run_id, wf)| (run_id.clone(), wf.clone()))
            .collect();
        let mut out = Vec::new();
        for (run_id, workflow) in &live {
            for kind in [ParkKind::Approval, ParkKind::Spawn] {
                let ctx = ParkCtx {
                    run_id: run_id.clone(),
                    workflow: workflow.name.clone(),
                    project: None,
                    kind,
                    candidates: park_candidates(workflow, kind),
                };
                out.extend(self.drain(engine, &ctx).await);
            }
        }
        out
    }

    /// Validate a claimed resolution: a match removes and returns the
    /// parked entry for the caller to settle; a mismatch is recorded and
    /// returned (never resumes anything).
    pub fn resolve(&mut self, res: Resolution) -> Result<PendingEntry, ResolutionMismatch> {
        let found = self.registry.get_latest(&res.run_id, &res.promise_id);
        match validate_resolution(found, &res) {
            Ok(()) => Ok(self
                .registry
                .remove(&res.run_id, &res.promise_id, res.generation)
                .expect("validated entry exists")),
            Err(m) => {
                self.ignored.push((res, m.clone()));
                Err(m)
            }
        }
    }

    /// Record a terminal publication attempt in the ledger.
    pub fn note_terminal(&mut self, record: TerminalRecord) -> TerminalNote {
        self.ledger.note(record)
    }

    /// Number of parked entries.
    pub fn registry_len(&self) -> usize {
        self.registry.len()
    }

    /// Entries parked for one run.
    pub fn entries_for_run(&self, run_id: &str) -> Vec<PendingEntry> {
        self.registry
            .entries_for_run(run_id)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Number of recorded (ignored) resolution mismatches.
    pub const fn ignored_count(&self) -> usize {
        self.ignored.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Throwaway journal store for host tests (mirrors the mcp-side helper;
    /// each crate owns its copy so the suites stay independent).
    async fn recon_store(tag: &str) -> toron_core::index::workflow_effects::WorkflowEffectStore {
        use toron_core::index::schema;
        let dir = std::env::temp_dir().join(format!("wf-recon-{tag}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("tmpdir");
        let db = dir.join("recon.sqlite");
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        toron_core::index::workflow_effects::WorkflowEffectStore::new(pool)
    }

    /// bd-graph-approval-host-dcqq: one wave can park several approvals, so
    /// every `AwaitingApproval` effect is registered under `(run, node)` and
    /// published — with its evidence payload — instead of the first one
    /// swallowing the rest.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_graph_multi_approval_parks_publish_per_node() {
        use crate::schema::Workflow;
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-graph-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host = WorkflowHost::over_bus(bus.clone(), vec![], None)
            .expect("host")
            .with_job_signer(Some(Keys::generate()));
        let wf = Workflow {
            name: "graph-approvals".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        let mut sub = bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");

        for node in ["aa", "zz"] {
            let payload = serde_json::json!({
                "run_id": "run-graph-2",
                "workflow": "graph-approvals",
                "suspended_at": { "node": node, "wave": 1 },
                "message": format!("approve {node}?"),
                "evidence_hash": format!("hash-{node}"),
            });
            host.park_durable_approval(
                "run-graph-2",
                &wf,
                &serde_json::json!({}),
                "digest-2",
                node.to_string(),
                format!("approve {node}?"),
                Some(payload),
            )
            .await;
        }

        {
            let parks = host.durable_parks.lock().unwrap();
            assert_eq!(parks.len(), 2, "two nodes, two parks: {parks:?}");
            assert!(parks.contains_key(&("run-graph-2".to_string(), "aa".to_string())));
            assert!(parks.contains_key(&("run-graph-2".to_string(), "zz".to_string())));
        }

        let mut nodes = Vec::new();
        for _ in 0..20 {
            if nodes.len() == 2 {
                break;
            }
            let Ok(got) = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await
            else {
                break;
            };
            let Some(toron_core::bus::SubscriptionEvent::Event(ev)) = got else {
                continue;
            };
            if ev.kind.as_u16() == 46011 {
                let body: serde_json::Value =
                    serde_json::from_str(&ev.content).expect("46011 json");
                let node = body["node"].as_str().expect("node key").to_string();
                assert_eq!(
                    body["payload"]["suspended_at"]["node"],
                    serde_json::json!(node),
                    "the graph evidence rides the event: {body}"
                );
                nodes.push(node);
            }
        }
        nodes.sort();
        assert_eq!(
            nodes,
            ["aa", "zz"],
            "one pending event per node, distinct node keys"
        );
    }

    /// The resolution lookup never cross-resolves: a named node finds its own
    /// park, an absent node falls back to the single run entry only, and the
    /// removal of one park leaves the sibling resolvable.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_graph_park_lookup_is_per_node_and_never_ambiguous() {
        use crate::schema::Workflow;
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db =
            std::env::temp_dir().join(format!("wf-graph-lookup-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host = WorkflowHost::over_bus(bus, vec![], None).expect("host");
        let wf = Workflow {
            name: "graph-lookup".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        for node in ["aa", "zz"] {
            host.park_durable_approval(
                "run-lookup",
                &wf,
                &serde_json::json!({}),
                "digest-3",
                node.to_string(),
                format!("approve {node}?"),
                None,
            )
            .await;
        }

        {
            let parks = host.durable_parks.lock().unwrap();
            assert_eq!(
                find_durable_park(&parks, "run-lookup", Some("zz"))
                    .expect("named node")
                    .node,
                "zz"
            );
            assert!(
                find_durable_park(&parks, "run-lookup", None).is_none(),
                "two parks + no node is ambiguous: never pick one"
            );
        }
        host.durable_parks
            .lock()
            .unwrap()
            .remove(&("run-lookup".to_string(), "aa".to_string()));
        {
            let parks = host.durable_parks.lock().unwrap();
            assert_eq!(parks.len(), 1, "the sibling stays parked");
            assert_eq!(
                find_durable_park(&parks, "run-lookup", Some("zz"))
                    .expect("sibling resolvable")
                    .node,
                "zz"
            );
            assert_eq!(
                find_durable_park(&parks, "run-lookup", None)
                    .expect("the linear-style fallback still works for one park")
                    .node,
                "zz"
            );
        }
    }

    /// bd-asx3 e2e: a real parked run is SUSPENDED (never failed), drains
    /// into the registry independently of the run await, and resumes to
    /// Succeeded through the validated host resolution path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn host_lifecycle_parked_run_suspends_and_resolves() {
        use crate::engine::ResolutionVerdict;
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-lifee2e-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let engine = WorkflowEngine::local().expect("engine");
        let host = WorkflowHost::over_bus_with_engine(bus, engine, vec![])
            .with_job_signer(Some(Keys::generate()));

        let wf = crate::schema::parse_workflow(
            "name: ship\ntrigger:\n  type: webhook\nsteps:\n  - name: gate\n    action: request_approval\n    message: Release?\n",
        )
        .expect("parse");
        let input = crate::engine::RunInput {
            workflow: wf.clone(),
            trigger: serde_json::json!({}),
            run_id: None,
        };
        host.spawn_run_and_drain("run-e2e-1".to_string(), wf, input)
            .await;

        let mut parked = false;
        for _ in 0..150 {
            let pending = host.pending().lock().await;
            if pending.run_state("run-e2e-1") == RunState::Suspended && pending.registry_len() == 1
            {
                parked = true;
                break;
            }
            drop(pending);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(parked, "run parks as Suspended with one structured entry");
        assert_ne!(
            host.pending().lock().await.run_state("run-e2e-1"),
            RunState::Failed,
            "suspended is distinct from failed"
        );
        let entries = host.pending().lock().await.entries_for_run("run-e2e-1");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].node_path, "gate");

        let res = crate::engine::Resolution {
            run_id: "run-e2e-1".into(),
            node: None,
            project: None,
            promise_id: entries[0].promise_id.clone(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: "ship it".into(),
        };
        host.resolve_parked(res).await.expect("resolve parked");

        let mut done = false;
        for _ in 0..150 {
            if host.pending().lock().await.run_state("run-e2e-1") == RunState::Succeeded {
                done = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(done, "run resumes to Succeeded after validated resolution");
        let _ = std::fs::remove_file(&db);
    }

    /// bd-aqvg: cursor persist/load round-trip through the host. Marks saved
    /// by one host boot-load into the next (duplicates bounded to the crash
    /// window); corrupt rows skip without failing the load; journal-less
    /// hosts skip both silently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cursor_persist_load_round_trip() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        let db = std::env::temp_dir().join(format!("wf-cur-rt-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        async fn mk_host(pool: &sqlx::SqlitePool) -> WorkflowHost {
            let bus: Arc<dyn Bus> = Arc::new(
                InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                    .await
                    .expect("bus"),
            );
            let h = WorkflowHost::over_bus(bus, vec![], None).expect("host");
            h.with_effect_store(WorkflowEffectStore::new(pool.clone()))
                .await;
            h
        }
        let bus0: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let h0 = WorkflowHost::over_bus(bus0, vec![], None).expect("host");
        h0.ensure_cursors_loaded().await;
        h0.persist_delivery_cursors().await;
        let h1 = mk_host(&pool).await;
        h1.delivery.mark_delivered("sess", ["id-1"]);
        assert!(h1.delivery.take_dirty(), "new mark dirties");
        h1.delivery.mark_delivered("sess-aux", ["x"]);
        h1.persist_delivery_cursors().await;
        let h2 = mk_host(&pool).await;
        h2.ensure_cursors_loaded().await;
        let (keep, _) = h2.delivery.reserve_undelivered("sess", ["id-1", "id-2"]);
        assert_eq!(
            keep,
            vec!["id-2".to_string()],
            "restart must not re-deliver id-1"
        );
        h2.ensure_cursors_loaded().await;
        h1.delivery.mark_delivered("sess", ["id-2"]);
        h1.flush_coalesced_bursts().await;
        assert!(!h1.delivery.take_dirty(), "tick consumed the dirty flag");
        let h4 = mk_host(&pool).await;
        h4.ensure_cursors_loaded().await;
        let (keep4, _) = h4.delivery.reserve_undelivered("sess", ["id-1", "id-2"]);
        assert!(keep4.is_empty(), "tick-persisted marks survive restart");
        sqlx::query(
            "INSERT INTO delivery_cursors (session, delivered_json, cursor, updated_us) \
             VALUES ('bad', 'not-json', 0, 0)",
        )
        .execute(&pool)
        .await
        .expect("seed corrupt");
        let h3 = mk_host(&pool).await;
        h3.ensure_cursors_loaded().await;
        let (keep3, _) = h3.delivery.reserve_undelivered("sess", ["id-1"]);
        assert!(keep3.is_empty(), "good rows still load past a corrupt row");
        let _ = std::fs::remove_file(&db);
    }

    /// bd-slkn: clean-stop persist covers every host at once. Marks set with
    /// no tick running survive through persist_hosts_cursors; hosts without
    /// a store save nothing and fail nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn graceful_stop_persists_all_hosts() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        let db = std::env::temp_dir().join(format!("wf-stop-all-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        async fn mk(pool: &sqlx::SqlitePool) -> WorkflowHost {
            let bus: Arc<dyn Bus> = Arc::new(
                InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                    .await
                    .expect("bus"),
            );
            let h = WorkflowHost::over_bus(bus, vec![], None).expect("host");
            h.with_effect_store(WorkflowEffectStore::new(pool.clone()))
                .await;
            h
        }
        let h1 = mk(&pool).await;
        h1.delivery.mark_delivered("sess", ["stop-1"]);
        let h2 = mk(&pool).await;
        let bus0: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let h0 = WorkflowHost::over_bus(bus0, vec![], None).expect("host");
        WorkflowHost::persist_hosts_cursors(&[h1, h0, h2]).await;
        let rows = WorkflowEffectStore::new(pool)
            .load_delivery_cursors()
            .await
            .expect("load");
        assert_eq!(rows.len(), 1);
        assert!(rows[0].1.contains("stop-1"), "stop persist saved the mark");
        let _ = std::fs::remove_file(&db);
    }

    /// bd-8x2o: loop end is a clean stop — marks set after the final tick
    /// persist instead of dying with the task. Closes the channel, awaits
    /// the task, reloads on the same pool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cursor_stop_persist() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        let db = std::env::temp_dir().join(format!("wf-cur-stop-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let h = WorkflowHost::over_bus(bus, vec![], None).expect("host");
        h.with_effect_store(WorkflowEffectStore::new(pool.clone()))
            .await;
        h.delivery.mark_delivered("sess", ["stop-1"]);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let handle = h.spawn_mail_trigger_loop(rx);
        drop(tx);
        handle.await.expect("loop task ends on closed channel");
        let rows = WorkflowEffectStore::new(pool)
            .load_delivery_cursors()
            .await
            .expect("load");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "sess");
        assert!(rows[0].1.contains("stop-1"), "stop hook persisted the mark");
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 T3: timer NoSigner parks DLQ (not warn+silent drop).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t3_timer_nosigner_parks_dlq() {
        use crate::coalesce::InboundMention;
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-t3-ns-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host = WorkflowHost::over_bus(bus, vec![], None).expect("host");
        {
            let mut c = host
                .coalescer()
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            c.submit(
                InboundMention {
                    assignee: "oc-t3".into(),
                    parent: "t3-parent".into(),
                    spec: json!({"t": 3}),
                },
                0,
            );
            let batch = c.force_flush("oc-t3").expect("flush");
            let _ = c.requeue(batch, 0, RequeueSchedule::FreshWindow);
        }
        host.flush_coalesced_bursts().await;
        let c = host
            .coalescer()
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(c.dlq_len(), 1, "timer NoSigner must park DLQ, not drop");
        assert_eq!(c.dlq_for("oc-t3")[0].reason, DlqReason::NoSigner);
        assert_eq!(c.buffered_for("oc-t3"), 0, "must not live-requeue NoSigner");
        drop(c);
        let _ = std::fs::remove_file(&db);
    }

    /// bd-host-cut-pqkg: durable resolutions validate before touching the
    /// state store; mismatches bail without settling anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_resolve_rejects_mismatches_before_the_store() {
        use crate::schema::Workflow;
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-cut-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host = WorkflowHost::over_bus(bus, vec![], None).expect("host");
        host.with_effect_store(recon_store("host-cut").await).await;
        host.durable_parks.lock().unwrap().insert(
            ("run-cut-9".to_string(), "approve-it".to_string()),
            DurablePark {
                node: "approve-it".into(),
                generation: 1,
                workflow_digest: "digest-9".into(),
                workflow: Workflow {
                    name: "linear-cut".into(),
                    description: None,
                    trigger: None,
                    policy: None,
                    guard: None,
                    principles: vec![],
                    steps: vec![],
                    graph: None,
                    inputs: vec![],
                    crew: None,
                },
                trigger: serde_json::json!({}),
            },
        );
        let bad_promise = Resolution {
            run_id: "run-cut-9".into(),
            node: None,
            project: None,
            promise_id: "promise-xyz".into(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: String::new(),
        };
        assert!(
            host.resolve_parked(bad_promise).await.is_err(),
            "promise id != run id must not settle a durable park"
        );
        let bad_generation = Resolution {
            run_id: "run-cut-9".into(),
            node: None,
            project: None,
            promise_id: "run-cut-9".into(),
            generation: 7,
            verdict: ResolutionVerdict::Approved,
            note: String::new(),
        };
        assert!(
            host.resolve_parked(bad_generation).await.is_err(),
            "a stale generation must not settle a durable park"
        );
        assert!(
            host.durable_parks
                .lock()
                .unwrap()
                .contains_key(&("run-cut-9".to_string(), "approve-it".to_string()))
        );
    }
    /// bd-host-cut-pqkg: the host-level restart witness — one linear workflow
    /// completes on the durable path with no vendor server anywhere.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_loop_completes_a_linear_workflow() {
        use crate::schema::{Action, Step, StepOnError, Workflow};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-cut-run-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let seen_digest: Arc<std::sync::Mutex<Option<String>>> =
            Arc::new(std::sync::Mutex::new(None));
        let digest_slot = seen_digest.clone();
        let afx: AsyncEffectRunner = Arc::new(move |inv: &ActionInvocation| {
            *digest_slot.lock().unwrap() = Some(inv.run.workflow_digest.clone());
            Box::pin(async move { Ok(serde_json::json!({ "ok": true })) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
                >
        });
        let workflow = Workflow {
            name: "linear-cut-run".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![Step {
                name: Some("notify".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::SendMessage {
                    to: vec!["nobody".into()],
                    subject: "cut".into(),
                    body: "hello".into(),
                    project: Some("demo".into()),
                },
            }],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        let host = WorkflowHost::over_bus(bus, vec![workflow.clone()], Some(afx)).expect("host");
        host.with_effect_store(recon_store("host-cut-run").await)
            .await;
        let input = RunInput {
            workflow: workflow.clone(),
            trigger: serde_json::json!({}),
            run_id: Some("run-cut-1".to_string()),
        };
        host.run_durable_loop("run-cut-1".to_string(), workflow.clone(), input)
            .await;
        let state = host
            .pending
            .lock()
            .await
            .run_state_durable("run-cut-1")
            .await;
        assert_eq!(state, RunState::Succeeded, "linear run completes durable");
        assert_eq!(
            seen_digest.lock().unwrap().as_deref(),
            Some(workflow_digest(&workflow).as_str()),
            "the runner sees the identity digest, never an empty string"
        );
    }

    /// bd-spawn-launcher-flip-d8mv witness 1: a spawn step on a host WITH a
    /// launcher launches exactly once, settles the full value, and completes —
    /// and kind-43001 is still published as operator evidence.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_spawn_launches_once_settles_and_completes() {
        use crate::schema::{Action, Step, StepOnError, Workflow};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;

        let db = std::env::temp_dir().join(format!("wf-spawn-run-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let mut sub = bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let launcher: SpawnLauncher = Arc::new(move |spec: SpawnSpec| {
            let seen = seen.clone();
            Box::pin(async move {
                seen.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"pane": "w1:p9", "spawn_key": spec.spawn_key}))
            })
        });
        let workflow = Workflow {
            name: "spawn-cut".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![Step {
                name: Some("spawn_it".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::Spawn {
                    profile: "coder".into(),
                    harness: None,
                    workflow: None,
                    await_: true,
                    no_claim_timeout: 600,
                    context: None,
                },
            }],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        let host = WorkflowHost::over_bus(bus, vec![workflow.clone()], None)
            .expect("host")
            .with_job_signer(Some(Keys::generate()))
            .with_spawn_seams(SpawnSeams {
                resolver: Some(Arc::new(|name: &str| {
                    (name == "coder").then(|| json!({"agent": "coder"}))
                })),
                launcher: Some(launcher),
            });
        host.with_effect_store(WorkflowEffectStore::new(pool.clone()))
            .await;
        let input = RunInput {
            workflow: workflow.clone(),
            trigger: json!({}),
            run_id: Some("run-spawn-1".to_string()),
        };
        host.run_durable_loop("run-spawn-1".to_string(), workflow.clone(), input)
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one launch");
        assert_eq!(
            host.pending
                .lock()
                .await
                .run_state_durable("run-spawn-1")
                .await,
            RunState::Succeeded,
            "the run completes"
        );
        let raw: String =
            sqlx::query_scalar("SELECT state_json FROM durable_workflow_runs WHERE run_id = ?1")
                .bind("run-spawn-1")
                .fetch_one(&pool)
                .await
                .expect("model row");
        let model: serde_json::Value = serde_json::from_str(&raw).expect("model json");
        assert_eq!(
            model["state"]["outputs"]["spawn_it"],
            json!({
                "ok": true,
                "spawn_key": "run-spawn-1:spawn_it:1",
                "result": {"pane": "w1:p9", "spawn_key": "run-spawn-1:spawn_it:1"},
            }),
            "the full launcher value settled the step"
        );

        let mut carried = None;
        for _ in 0..20 {
            let Ok(got) = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await
            else {
                break;
            };
            let Some(toron_core::bus::SubscriptionEvent::Event(ev)) = got else {
                continue;
            };
            if ev.kind.as_u16() == 43001 {
                let body: serde_json::Value = serde_json::from_str(&ev.content).expect("job json");
                carried = Some(body);
                break;
            }
        }
        let body = carried.expect("a 43001 spawn job");
        assert_eq!(body["spec"]["spawn_key"], json!("run-spawn-1:spawn_it:1"));
        assert_eq!(body["spec"]["profile"], json!("coder"));
    }

    /// bd-spawn-launcher-flip-d8mv witness 4: a launcher that never resolves
    /// fails the node at the park deadline with the OLD timeout text.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_spawn_launcher_timeout_keeps_the_old_error_text() {
        use crate::schema::{Action, Step, StepOnError, Workflow};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;

        let db =
            std::env::temp_dir().join(format!("wf-spawn-timeout-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let launcher: SpawnLauncher = Arc::new(|_spec: SpawnSpec| {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                Ok(json!({"pane": "never"}))
            })
        });
        let workflow = Workflow {
            name: "spawn-timeout".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![Step {
                name: Some("spawn_it".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::Spawn {
                    profile: "coder".into(),
                    harness: None,
                    workflow: None,
                    await_: true,
                    no_claim_timeout: 1,
                    context: None,
                },
            }],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        let host = WorkflowHost::over_bus(bus, vec![workflow.clone()], None)
            .expect("host")
            .with_spawn_seams(SpawnSeams {
                resolver: Some(Arc::new(|name: &str| {
                    (name == "coder").then(|| json!({"agent": "coder"}))
                })),
                launcher: Some(launcher),
            });
        host.with_effect_store(WorkflowEffectStore::new(pool.clone()))
            .await;
        let input = RunInput {
            workflow: workflow.clone(),
            trigger: json!({}),
            run_id: Some("run-spawn-t".to_string()),
        };
        host.run_durable_loop("run-spawn-t".to_string(), workflow.clone(), input)
            .await;

        let raw: String =
            sqlx::query_scalar("SELECT state_json FROM durable_workflow_runs WHERE run_id = ?1")
                .bind("run-spawn-t")
                .fetch_one(&pool)
                .await
                .expect("model row");
        let model: serde_json::Value = serde_json::from_str(&raw).expect("model json");
        assert_eq!(
            model["state"]["outputs"]["spawn_it"],
            json!({
                "ok": false,
                "error": "spawn-await run-spawn-t:spawn_it:1: timed out after 1s",
            }),
            "the old promise-timeout text, keyed by spawn_key"
        );
        assert_eq!(
            host.pending
                .lock()
                .await
                .run_state_durable("run-spawn-t")
                .await,
            RunState::Succeeded,
            "on_error: continue lands the value and the run completes"
        );
    }

    /// bd-host-cut-pqkg: the restart witness — host1 parks an approval and
    /// dies; host2's sweep re-opens the persisted model and re-drives it to
    /// a fresh park; a resolution then settles the run through the store.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_sweep_resumes_a_parked_run_after_restart() {
        use crate::schema::{Action, Step, StepOnError, Workflow};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-cut-sweep-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let approval_workflow = Workflow {
            name: "linear-cut-approval".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![Step {
                name: Some("approve-it".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::RequestApproval {
                    message: "ship it?".into(),
                },
            }],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        let bus1: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host1 =
            WorkflowHost::over_bus(bus1, vec![approval_workflow.clone()], None).expect("host1");
        host1
            .with_effect_store(
                toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
            )
            .await;
        let input = RunInput {
            workflow: approval_workflow.clone(),
            trigger: serde_json::json!({}),
            run_id: Some("run-cut-9".to_string()),
        };
        host1
            .run_durable_loop("run-cut-9".to_string(), approval_workflow.clone(), input)
            .await;
        assert_eq!(
            host1
                .pending
                .lock()
                .await
                .run_state_durable("run-cut-9")
                .await,
            RunState::Suspended,
            "host1 parks the approval"
        );
        drop(host1);
        let bus2: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host2 = WorkflowHost::over_bus(bus2, vec![], None).expect("host2");
        host2
            .with_effect_store(
                toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
            )
            .await;
        let _sweep = host2.spawn_durable_sweep(std::time::Duration::from_secs(3600));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if host2
                .durable_parks
                .lock()
                .unwrap()
                .keys()
                .any(|(rid, _)| rid == "run-cut-9")
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "sweep must re-park the restarted run"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let res = Resolution {
            run_id: "run-cut-9".into(),
            node: None,
            project: None,
            promise_id: "run-cut-9".into(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: "yes".into(),
        };
        host2.resolve_parked(res).await.expect("durable resolve");
        let state = host2
            .pending
            .lock()
            .await
            .run_state_durable("run-cut-9")
            .await;
        assert_eq!(
            state,
            RunState::Succeeded,
            "resolution settles the swept run"
        );
    }

    /// The DAG cut witness: a graph approval parked before the restart must
    /// decode as a `GraphModel` on the sweep, rebuild its `durable_parks`
    /// entry, and settle on resolution. Before the tagged model, every DAG
    /// row was parsed as linear and left behind as "unparsable".
    #[tokio::test]
    async fn durable_graph_sweep_rebuilds_park_after_restart() {
        use crate::schema::{Action, Graph, GraphNode, StepOnError, Workflow};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-graph-cut-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let workflow = Workflow {
            name: "graph-cut-approval".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![],
            graph: Some(Graph {
                nodes: [(
                    "approve".to_string(),
                    GraphNode {
                        action: Action::RequestApproval {
                            message: "ship the DAG?".into(),
                        },
                        needs: vec![],
                        guard: None,
                        on_error: StepOnError::Continue,
                        build_slot: false,
                    },
                )]
                .into(),
            }),
            inputs: vec![],
            crew: None,
        };
        let bus1: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host1 = WorkflowHost::over_bus(bus1, vec![workflow.clone()], None).expect("host1");
        host1
            .with_effect_store(
                toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
            )
            .await;
        let input = RunInput {
            workflow: workflow.clone(),
            trigger: serde_json::json!({}),
            run_id: Some("run-graph-cut-9".to_string()),
        };
        host1
            .run_durable_graph_loop("run-graph-cut-9".to_string(), workflow, input)
            .await;
        assert_eq!(
            host1
                .pending
                .lock()
                .await
                .run_state_durable("run-graph-cut-9")
                .await,
            RunState::Suspended,
            "host1 parks the graph approval"
        );
        assert!(
            host1
                .durable_parks
                .lock()
                .unwrap()
                .contains_key(&("run-graph-cut-9".to_string(), "approve".to_string())),
            "the graph park is keyed by node"
        );
        drop(host1);

        let bus2: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host2 = WorkflowHost::over_bus(bus2, vec![], None).expect("host2");
        host2
            .with_effect_store(
                toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
            )
            .await;
        let _sweep = host2.spawn_durable_sweep(std::time::Duration::from_secs(3600));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if host2
                .durable_parks
                .lock()
                .unwrap()
                .contains_key(&("run-graph-cut-9".to_string(), "approve".to_string()))
            {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "graph sweep must re-park the restarted run"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let res = Resolution {
            run_id: "run-graph-cut-9".into(),
            node: Some("approve".into()),
            project: None,
            promise_id: "run-graph-cut-9".into(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: "yes".into(),
        };
        host2.resolve_parked(res).await.expect("graph resolve");
        assert_eq!(
            host2
                .pending
                .lock()
                .await
                .run_state_durable("run-graph-cut-9")
                .await,
            RunState::Succeeded,
            "resolution settles the swept graph run"
        );
    }

    /// bd-host-cut-pqkg: the cut witness. A step settled before the restart
    /// must not run again after it — the model resumes at the parked step,
    /// not the beginning. Counts dispatches on each host's runner so a
    /// replayed settled step is visible, not inferred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_does_not_replay_a_settled_step() {
        use crate::schema::{Action, Step, StepOnError, Workflow};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        fn counting_runner(count: Arc<AtomicUsize>) -> AsyncEffectRunner {
            Arc::new(move |_inv: &ActionInvocation| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(serde_json::json!({ "ok": true })) })
                    as std::pin::Pin<
                        Box<
                            dyn std::future::Future<Output = anyhow::Result<serde_json::Value>>
                                + Send,
                        >,
                    >
            })
        }

        let db =
            std::env::temp_dir().join(format!("wf-cut-replay-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let workflow = Workflow {
            name: "linear-cut-replay".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![
                Step {
                    name: Some("settled-before-restart".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::SendMessage {
                        to: vec!["nobody".into()],
                        subject: "once".into(),
                        body: "settled".into(),
                        project: Some("demo".into()),
                    },
                },
                Step {
                    name: Some("approve-it".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::RequestApproval {
                        message: "ship it?".into(),
                    },
                },
            ],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        let before = Arc::new(AtomicUsize::new(0));
        let bus1: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus1"),
        );
        let host1 = WorkflowHost::over_bus(
            bus1,
            vec![workflow.clone()],
            Some(counting_runner(before.clone())),
        )
        .expect("host1");
        host1
            .with_effect_store(
                toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
            )
            .await;
        host1
            .run_durable_loop(
                "run-cut-replay".to_string(),
                workflow.clone(),
                RunInput {
                    workflow: workflow.clone(),
                    trigger: serde_json::json!({}),
                    run_id: Some("run-cut-replay".to_string()),
                },
            )
            .await;
        assert_eq!(
            before.load(Ordering::SeqCst),
            1,
            "step 0 ran once before the park"
        );
        assert!(
            host1
                .durable_parks
                .lock()
                .unwrap()
                .keys()
                .any(|(rid, _)| rid == "run-cut-replay"),
            "host1 parked on the approval"
        );
        drop(host1);

        let after = Arc::new(AtomicUsize::new(0));
        let bus2: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus2"),
        );
        let host2 = WorkflowHost::over_bus(bus2, vec![], Some(counting_runner(after.clone())))
            .expect("host2");
        host2
            .with_effect_store(
                toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
            )
            .await;
        let _sweep = host2.spawn_durable_sweep(std::time::Duration::from_secs(3600));
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while !host2
            .durable_parks
            .lock()
            .unwrap()
            .keys()
            .any(|(rid, _)| rid == "run-cut-replay")
        {
            assert!(
                tokio::time::Instant::now() < deadline,
                "sweep must re-park the restarted run"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        assert_eq!(
            after.load(Ordering::SeqCst),
            0,
            "the settled step must not be re-dispatched after restart"
        );

        host2
            .resolve_parked(Resolution {
                run_id: "run-cut-replay".into(),
                node: None,
                project: None,
                promise_id: "run-cut-replay".into(),
                generation: 1,
                verdict: ResolutionVerdict::Approved,
                note: "yes".into(),
            })
            .await
            .expect("durable resolve");
        assert_eq!(
            host2
                .pending
                .lock()
                .await
                .run_state_durable("run-cut-replay")
                .await,
            RunState::Succeeded
        );
        assert_eq!(
            after.load(Ordering::SeqCst),
            0,
            "settling the rest of the run replays no settled step"
        );
        let _ = std::fs::remove_file(db);
    }

    /// bd-host-cut-verb-fail-vdhb: a corrupt model cannot resume, so its open
    /// settles the run once instead of leaving the sweep to retry a row that
    /// can never parse.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn corrupt_durable_model_settles_failed_instead_of_retrying() {
        use crate::schema::{Action, Step, StepOnError, Workflow};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::durable_workflow::WorkflowStateStore;
        use toron_core::index::schema;

        let db =
            std::env::temp_dir().join(format!("wf-cut-corrupt-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        WorkflowStateStore::new(pool.clone())
            .save("run-corrupt", "{ this is not a run model")
            .await
            .expect("save corrupt row");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let host = WorkflowHost::over_bus(bus, vec![], None).expect("host");
        host.with_effect_store(
            toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone()),
        )
        .await;
        let workflow = Workflow {
            name: "corrupt-cut".into(),
            description: None,
            trigger: None,
            policy: None,
            guard: None,
            principles: vec![],
            steps: vec![Step {
                name: Some("never-runs".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::SendMessage {
                    to: vec!["nobody".into()],
                    subject: "corrupt".into(),
                    body: "never".into(),
                    project: None,
                },
            }],
            graph: None,
            inputs: vec![],
            crew: None,
        };
        host.run_durable_loop(
            "run-corrupt".to_string(),
            workflow.clone(),
            RunInput {
                workflow,
                trigger: serde_json::json!({}),
                run_id: Some("run-corrupt".to_string()),
            },
        )
        .await;
        assert_eq!(
            host.pending
                .lock()
                .await
                .run_state_durable("run-corrupt")
                .await,
            RunState::Failed,
            "a corrupt model settles once; the sweep must not retry it forever"
        );
        let _ = std::fs::remove_file(db);
    }
}
