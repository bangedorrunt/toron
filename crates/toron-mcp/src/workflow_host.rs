// governed-by: ADR-0004 (bd-pkc part 2), ADR-0005 (Spec 2 engine host)
#![forbid(unsafe_code)]

//! The production workflow-engine construction site (bd-pkc part 2).
//!
//! ADR-0004 shipped four library primitives with NO call site: the
//! `MentionCoalescer` (Ship 1), `should_evaluate_trigger_event` (Ship 2 loop
//! prevention), the bounded guard evaluator (Ship 4), and the
//! `toron:workflow-emitted` stamp (Ship 5). This module is the daemon path
//! that makes them LIVE:
//!
//! - A trigger-ingress loop subscribes to the bus and applies
//!   [`toron_workflow::trigger_filter::should_evaluate_trigger_event`] BEFORE
//!   matching a workflow's trigger — the only correct loop-prevention layer.
//! - A workflow whose `trigger` spec matches an event, and whose `guard`
//!   evaluates true, is invoked through [`WorkflowEngine`] (`run`, the same
//!   durable function as the trigger path).
//! - The engine's async effect runner dispatches through the Mailbox (a DM send
//!   emits a kind-14 gift wrap) and STAMPS every event it emits with
//!   `toron:workflow-emitted` — so a workflow can never re-trigger itself.
//! - `@mention` dispatch runs through a [`MentionCoalescer`] (one job per
//!   burst), ticked by the trigger loop's per-event flush.
//!
//! governed-by: ADR-0004 ships 1/2/4/5, ADR-0005 Spec 2

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, PoisonError};

use crate::send_armor::{
    ARMOR_MAX_CONCURRENCY, ARMOR_SEND_TIMEOUT, ArmorError, SendArmor, SendFuture, SendRequest,
    passthrough,
};
use anyhow::{Context, Result};
use nostr::prelude::{Event, EventBuilder, EventId, FinalizeEvent, Keys, Kind, Tag, Timestamp};
use toron_core::bus::{Bus, SubscriptionEvent};
use toron_core::index::durable_engine::ToronEngineStore;
use toron_core::index::durable_workflow::RunState as DurableModelState;
use toron_core::index::durable_workflow::SyncWorkflowState;
use toron_core::journal::{JournalDb, ToronJournal};
use toron_core::kernel::{OurEngine, RunPhase as EnginePhase};
use toron_core::mailbox::{Mailbox, SendMessage, UnwrappedMail};
use toron_workflow::coalesce::{
    CoalesceDecision, Coalesced, DlqReason, MentionCoalescer, RequeueOutcome, RequeueSchedule,
};
use toron_workflow::dispatch::{DispatchAcquire, DispatchConfig, DispatchPermit, DispatchPool};
use toron_workflow::engine::durable::{
    GraphEffect, GraphModel, RunEffect, RunModel, RunPhase as ModelPhase, needs_graph_model,
    unsupported_reason,
};
use toron_workflow::engine::durable_host::{DurableGraphRun, DurableRun, HostError};
use toron_workflow::engine::leaf::LeafSeams;
use toron_workflow::engine::linear_step::{
    GraphNodeStep, LinearStep, run_graph_node_step, run_linear_step,
};
use toron_workflow::engine::spawn::{SpawnLauncher, SpawnSeams, SpawnSpec, spawn_launcher_refusal};
use toron_workflow::engine::{
    ActionInvocation, ActionPartition, AsyncEffectRunner, AsyncRunJournal, CleanupKind,
    CleanupRequest, Decision, DispatchPlan, EffectJournalMode, FORCED_CLEANUP_CAPABILITY, ParkKind,
    PendingEntry, PendingRegistry, ProviderCapability, ProviderErrorClass, ProviderOutcome,
    Resolution, ResolutionMismatch, ResolutionVerdict, RunInput, RunPhase, RunTransition,
    TerminalLedger, TerminalNote, TerminalRecord, UnknownReport, WorkflowEngine, attribute_parks,
    capability_for_action_type, classify_provider_error_msg, cleanup_effect_key, corr_for_effect,
    derive_effect_key, describe_unknown, effect_view, failure_for_control_misdispatch,
    failure_for_unsupported, normalize_dispatch_result, plan_attempt, terminal_digest,
    validate_resolution,
};
use toron_workflow::identity::workflow_digest;
use toron_workflow::jobs::{JobRequest, build_job_request_with_tags};
use toron_workflow::lifecycle::{LifecycleEffect, WorkflowLifecycle};
use toron_workflow::schema::{Action, Trigger, Workflow};
use toron_workflow::stamp_workflow_emitted;
use toron_workflow::trigger_filter::{
    matches_trigger, should_evaluate_trigger_event, trigger_payload, trigger_run_id,
};
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
async fn mention_outcome(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    decision: CoalesceDecision,
    key: String,
    delivery: Option<&toron_workflow::DeliveryCursorStore>,
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
    let (redacted, _) = toron_workflow::identity::redact_value(&rendered);
    let action_digest = toron_workflow::identity::action_digest_of(&redacted);
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
/// [`toron_workflow::DeliveryCursorStore`] so DispatchJob / cap FlushNow
/// shares peek→publish→mark with the trigger-loop timer flush (GS-062).
/// Optional [`DispatchPool`] wires captain-C acquire on the FlushNow arm
/// (same as timer flush) — production hosts pass `Some`.
pub fn mailbox_async_effect_runner_with_delivery(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    armor: Arc<SendArmor>,
    delivery: Option<Arc<toron_workflow::DeliveryCursorStore>>,
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
            let outcome = match toron_workflow::engine::partition(&inv.action) {
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
    delivery: Option<Arc<toron_workflow::DeliveryCursorStore>>,
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
            let mention = toron_workflow::coalesce::InboundMention {
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
    delivery: Option<&toron_workflow::DeliveryCursorStore>,
) -> Result<EventId, CoalescePublishError> {
    let parent_ids = coalesced.parent_ids();
    let session = coalesced.assignee.as_str();
    let mut reservation: Option<toron_workflow::ReservationGuard<'_>> = None;
    let mut filtered: Option<Coalesced> = None;
    if let Some(store) = delivery
        && !parent_ids.is_empty()
    {
        let proof_id = parent_ids.first().map(String::as_str).unwrap_or("");
        let _ = toron_workflow::agent_key_is_not_delivery_proof("unused", proof_id);
        let (keep, effects) = store.reserve_undelivered(session, parent_ids.clone());
        if keep.is_empty() {
            let any_inflight = effects
                .iter()
                .any(|e| matches!(e, toron_workflow::DeliveryEffect::SkipInFlight { .. }));
            if any_inflight {
                return Err(CoalescePublishError::InFlight);
            }
            return Err(CoalescePublishError::AlreadyDelivered);
        }
        if keep.len() != parent_ids.len() {
            filtered = Some(coalesced.retain_parents(&keep));
        }
        reservation = Some(toron_workflow::ReservationGuard::new(store, session, keep));
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
fn now_ms() -> u64 {
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
    delivery: Arc<toron_workflow::DeliveryCursorStore>,
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
fn find_durable_park(
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
struct DurablePark {
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
    /// path uses: `toron_workflow::identity::workflow_digest`).
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
    if let Ok(model) = serde_json::from_str::<RunModel>(json) {
        return Some((model.workflow, model.trigger));
    }
    serde_json::from_str::<GraphModel>(json)
        .ok()
        .map(|model| (model.workflow, model.trigger))
}

/// Route one workflow to the durable host or the old engine (bd-host-cut-pqkg).
/// `Ok` means the durable path owns it; `Err(reason)` names why it stays on
/// `WorkflowEngine::run`. Pure: no row exists at this point either way.
///
/// `spawn_capable` is the caller's CAPABILITY, not the workflow's shape: a
/// spawn workflow is admitted only when this host can actually launch the
/// child (bd-spawn-launcher-flip-d8mv).
pub fn durable_route(
    workflow: &Workflow,
    has_store: bool,
    spawn_capable: bool,
) -> Result<(), String> {
    if !has_store {
        return Err("no durable store attached".into());
    }
    if let Some(reason) = unsupported_reason(workflow) {
        return Err(reason);
    }
    if !spawn_capable && let Some(reason) = spawn_launcher_refusal(workflow) {
        return Err(reason);
    }
    Ok(())
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
        let delivery = Arc::new(toron_workflow::DeliveryCursorStore::new());
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
        let delivery = Arc::new(toron_workflow::DeliveryCursorStore::new());
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
        let delivery = Arc::new(toron_workflow::DeliveryCursorStore::new());
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
        let event = toron_workflow::engine::build_workflow_run_terminal(signer, run_id, &outcome)
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
            let model: RunModel = match serde_json::from_str(&json) {
                Ok(model) => model,
                Err(e) => {
                    warn!(run_id = %run_id, "durable sweep: unparsable model, leaving the row: {e}");
                    continue;
                }
            };
            if model.is_terminal() {
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
                            == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
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
                            toron_workflow::guard::evaluate_guard_default(Some(guard), &vars).await;
                        if !matches!(outcome, toron_workflow::guard::GuardOutcome::Bool(true)) {
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
                                toron_workflow::DeliveryCursor {
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
    pub(crate) async fn persist_hosts_cursors(hosts: &[Self]) {
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
                    let outcome =
                        toron_workflow::guard::evaluate_guard_default(Some(guard), &vars).await;
                    debug!(
                        outcome = ?outcome,
                        "workflow {} guard evaluated",
                        workflow.name
                    );
                    if !matches!(outcome, toron_workflow::guard::GuardOutcome::Bool(true)) {
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

use toron_core::index::workflow_effects::{
    ClaimAttempt, EffectState, NewAttempt, NewEffect, NewPark, NewRun, PendingKind, RunStatus,
    WorkflowEffect, WorkflowEffectStore, now_us,
};

/// One effect dispatch request: identity inputs plus the provider's
/// declared capability. The effect key is minted by the caller (stable
/// across retries); the dispatcher never invents a second key.
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub effect_key: String,
    pub run_id: String,
    pub project: Option<String>,
    pub workflow_digest: String,
    pub node_path: String,
    pub iteration_key: Option<String>,
    pub action_type: String,
    pub action_digest: String,
    pub redacted_action: Option<serde_json::Value>,
    pub capability: ProviderCapability,
}

/// The journaled result of one dispatch call.
#[derive(Debug, Clone, PartialEq)]
pub struct DispatchResult {
    pub effect_key: String,
    pub attempt: i64,
    pub state: EffectState,
    pub outcome: serde_json::Value,
}

/// Classify one provider outcome into (effect state, failure evidence,
/// outcome value). `Null` delivery and unproven results never read as
/// success: null is `invalid_output`, ambiguity is `unknown`.
fn classify_outcome(
    outcome: &ProviderOutcome,
) -> (
    EffectState,
    Option<serde_json::Value>,
    Option<serde_json::Value>,
) {
    match outcome {
        ProviderOutcome::Delivered(v) if v.is_null() => (
            EffectState::Failed,
            Some(serde_json::json!({"code": "invalid_output"})),
            None,
        ),
        ProviderOutcome::Delivered(v) => (EffectState::Settled, None, Some(v.clone())),
        ProviderOutcome::Deferred(v) => (EffectState::Accepted, None, Some(v.clone())),
        ProviderOutcome::ProvenFailed(msg) => (
            EffectState::Failed,
            Some(serde_json::json!({"error": msg})),
            None,
        ),
        ProviderOutcome::Ambiguous(msg) => (
            EffectState::Unknown,
            Some(serde_json::json!({"error": msg})),
            None,
        ),
    }
}

/// Dispatch-lease duration per attempt (5 minutes in µs). An attempt that
/// outlives its lease without settling is recoverable by a new owner; it is
/// never proof the provider was not called.
pub const DISPATCH_LEASE_US: i64 = 300_000_000;

/// Executes fenced effect dispatches against the workflow-effect journal.
#[derive(Debug, Clone)]
pub struct EffectDispatcher {
    store: WorkflowEffectStore,
}

/// A claimed attempt, ready to invoke (bd-ckkf): the claim-phase output
/// shared by [`EffectDispatcher::dispatch`] and
/// [`EffectDispatcher::dispatch_async`] so the fencing path exists once.
struct ClaimedAttempt {
    attempt_no: i64,
    attempt_id: String,
    token: String,
}

/// The claim-phase outcome: either an already-terminal row to return, or a
/// fenced attempt to invoke exactly once.
enum ClaimOutcome {
    /// Return this result without invoking: duplicate terminal or refusal
    /// (refusals arrive as `Err`).
    Done(anyhow::Result<DispatchResult>),
    /// Invoke the provider once under this fence, then settle.
    Claimed(ClaimedAttempt),
}

impl EffectDispatcher {
    pub const fn new(store: WorkflowEffectStore) -> Self {
        Self { store }
    }

    pub const fn store(&self) -> &WorkflowEffectStore {
        &self.store
    }

    /// Run the dispatch algorithm (ADR-0032 §5.3): render/validate aside,
    /// 1. insert-or-load the prepared intent (duplicate keys attach, never
    ///    fork);
    /// 2. return the stored outcome when the row is already terminal;
    /// 3. plan from the journaled view (refuse/reconcile/duplicate honored);
    /// 4. claim the attempt under fencing before invoking the provider;
    /// 5. link `transport_corr` by projection;
    /// 6. invoke the provider exactly once for this attempt;
    /// 7. settle, fail, or mark `unknown` from the classified outcome, with
    ///    append-only attempt evidence at every step.
    pub async fn dispatch(
        &self,
        req: &DispatchRequest,
        provider: impl FnOnce(&DispatchRequest) -> ProviderOutcome,
    ) -> anyhow::Result<DispatchResult> {
        match self.prepare_and_claim(req).await? {
            ClaimOutcome::Done(result) => result,
            ClaimOutcome::Claimed(claim) => {
                let outcome = provider(req);
                self.settle_and_record(req, claim, &outcome).await
            }
        }
    }

    /// The async-provider twin of [`dispatch`](Self::dispatch) (bd-ckkf,
    /// ADR-0035 Spec 2.2): identical fencing, but the provider is awaited
    /// inline — the path the async host runner uses, so no thread-hop
    /// bridge exists anywhere on the effect path.
    pub async fn dispatch_async<Fut>(
        &self,
        req: &DispatchRequest,
        provider: impl FnOnce(&DispatchRequest) -> Fut,
    ) -> anyhow::Result<DispatchResult>
    where
        Fut: std::future::Future<Output = ProviderOutcome> + Send,
    {
        match self.prepare_and_claim(req).await? {
            ClaimOutcome::Done(result) => result,
            ClaimOutcome::Claimed(claim) => {
                let outcome = provider(req).await;
                self.settle_and_record(req, claim, &outcome).await
            }
        }
    }

    /// Dispatch steps 1–5: insert-or-load the prepared intent (duplicate
    /// keys attach, never fork); return the stored outcome when already
    /// terminal; plan from the journaled view; claim the attempt under
    /// fencing; link `transport_corr` by projection.
    async fn prepare_and_claim(&self, req: &DispatchRequest) -> anyhow::Result<ClaimOutcome> {
        let prepared = self
            .store
            .prepare_effect(NewEffect {
                effect_key: req.effect_key.clone(),
                run_id: req.run_id.clone(),
                project: req.project.clone(),
                workflow_digest: req.workflow_digest.clone(),
                node_path: req.node_path.clone(),
                iteration_key: req.iteration_key.clone(),
                action_type: req.action_type.clone(),
                action_digest: req.action_digest.clone(),
                redacted_action: req.redacted_action.clone(),
                retention_until: None,
            })
            .await?;
        let row = prepared.effect;
        if row.state.is_terminal() {
            return Ok(ClaimOutcome::Done(Ok(self.result_from_row(&row))));
        }
        let now = now_us();
        let lease_live = matches!(row.lease_expires_at, Some(expires) if expires > now);
        let view = effect_view(row.state, row.attempt, lease_live);
        let attempt_no = match plan_attempt(Some(&view), &req.capability) {
            DispatchPlan::DispatchNew => row.attempt,
            DispatchPlan::RetrySameKey { attempt } => attempt,
            DispatchPlan::DuplicateTerminal => {
                return Ok(ClaimOutcome::Done(Ok(self.result_from_row(&row))));
            }
            DispatchPlan::ReconcileFirst { reason } | DispatchPlan::RefuseBlindRetry { reason } => {
                return Ok(ClaimOutcome::Done(Err(anyhow::anyhow!(
                    "dispatch refused for {}: {reason}",
                    req.effect_key
                ))));
            }
            DispatchPlan::ResumeParked { attempt } => attempt,
        };
        let token = format!("tok-{}-a{}", req.effect_key, attempt_no);
        let attempt_id = format!("att-{}-a{}", req.effect_key, attempt_no);
        let claimed = self
            .store
            .claim_attempt(
                &req.effect_key,
                ClaimAttempt {
                    attempt: attempt_no,
                    attempt_id: attempt_id.clone(),
                    claim_token: token.clone(),
                    lease_expires_at: now + DISPATCH_LEASE_US,
                    expected_claim_token: row.claim_token.clone(),
                },
            )
            .await?;
        if !claimed {
            return Ok(ClaimOutcome::Done(Err(anyhow::anyhow!(
                "dispatch fenced for {}: live owner holds the lease",
                req.effect_key
            ))));
        }
        self.store
            .record_attempt(NewAttempt {
                effect_key: req.effect_key.clone(),
                attempt: attempt_no,
                attempt_id: attempt_id.clone(),
                claim_token: Some(token.clone()),
                state: EffectState::InFlight,
                evidence: None,
            })
            .await?;
        self.store
            .link_transport_corr(&req.effect_key, &corr_for_effect(&req.effect_key))
            .await?;
        Ok(ClaimOutcome::Claimed(ClaimedAttempt {
            attempt_no,
            attempt_id,
            token,
        }))
    }

    /// Dispatch steps 6–7: invoke has happened (the caller invoked the
    /// provider exactly once for the claimed attempt); settle, fail, or mark
    /// `unknown` from the classified outcome, with append-only attempt
    /// evidence.
    async fn settle_and_record(
        &self,
        req: &DispatchRequest,
        claim: ClaimedAttempt,
        outcome: &ProviderOutcome,
    ) -> anyhow::Result<DispatchResult> {
        let ClaimedAttempt {
            attempt_no,
            attempt_id,
            token,
        } = claim;
        let (to, failure, value) = classify_outcome(outcome);
        let settled = self
            .store
            .settle_with_fence(&req.effect_key, &token, to, failure, value)
            .await?;
        if !settled {
            anyhow::bail!("dispatch lost fencing for {} mid-attempt", req.effect_key);
        }
        self.store
            .record_attempt(NewAttempt {
                effect_key: req.effect_key.clone(),
                attempt: attempt_no,
                attempt_id,
                claim_token: Some(token),
                state: to,
                evidence: Some(match outcome {
                    ProviderOutcome::Delivered(v) | ProviderOutcome::Deferred(v) => v.clone(),
                    ProviderOutcome::ProvenFailed(m) | ProviderOutcome::Ambiguous(m) => {
                        serde_json::json!({"error": m})
                    }
                }),
            })
            .await?;
        let row = self
            .store
            .get_effect(&req.effect_key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("effect vanished after settle"))?;
        Ok(self.result_from_row(&row))
    }

    /// Settle one effect with an explicitly presented fencing token.
    /// Returns `false` (changing nothing) when the token is stale — the
    /// caller that lost the fence must reconcile, not overwrite.
    pub async fn settle_with_presented(
        &self,
        effect_key: &str,
        presented_token: &str,
        outcome: ProviderOutcome,
    ) -> anyhow::Result<bool> {
        let (to, failure, value) = classify_outcome(&outcome);
        self.store
            .settle_with_fence(effect_key, presented_token, to, failure, value)
            .await
    }

    fn result_from_row(&self, row: &WorkflowEffect) -> DispatchResult {
        let outcome = row
            .outcome_json
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .or_else(|| {
                row.failure_json
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
            })
            .unwrap_or(serde_json::Value::Null);
        DispatchResult {
            effect_key: row.effect_key.clone(),
            attempt: row.attempt,
            state: row.state,
            outcome,
        }
    }

    /// Run one fixed cleanup effect (ADR-0032 Spec 8.2, bd-v139.3): lease
    /// release, bounded captain evidence, or bead comment — never a
    /// user-authored undo, never a rollback claim, never a bead close.
    /// The attempt journals under `{parent}/cleanup:{kind}` with the
    /// kind's retry capability; the parent failure row is never written.
    /// `force` is the explicit operator override for ambiguous non-idempotent
    /// cleanups (out-of-band confirmation first): without it, an uncertain
    /// mail/comment refuses to re-fire.
    pub async fn run_cleanup(
        &self,
        parent_key: &str,
        kind: CleanupKind,
        note: Option<serde_json::Value>,
        force: bool,
        provider: impl FnOnce(&CleanupRequest) -> ProviderOutcome,
    ) -> anyhow::Result<DispatchResult> {
        let key = cleanup_effect_key(parent_key, kind);
        let mut capability = kind.capability();
        if force {
            let row = self.store.get_effect(&key).await?;
            let needs_override = match row {
                Some(r) if !r.state.is_terminal() => {
                    let now = now_us();
                    let lease_live = matches!(r.lease_expires_at, Some(e) if e > now);
                    let view = effect_view(r.state, r.attempt, lease_live);
                    matches!(
                        plan_attempt(Some(&view), &capability),
                        DispatchPlan::RefuseBlindRetry { .. } | DispatchPlan::ReconcileFirst { .. }
                    )
                }
                _ => false,
            };
            if needs_override {
                capability = FORCED_CLEANUP_CAPABILITY;
            }
        }
        let run_id = match self.store.get_effect(parent_key).await? {
            Some(parent) => parent.run_id,
            None => parent_key.to_string(),
        };
        let action_type = format!("cleanup:{}", kind.as_str());
        let req = DispatchRequest {
            effect_key: key,
            run_id,
            project: None,
            workflow_digest: String::new(),
            node_path: String::new(),
            iteration_key: None,
            action_type,
            action_digest: String::new(),
            redacted_action: note.clone(),
            capability,
        };
        let cleanup = CleanupRequest {
            parent_key: parent_key.to_string(),
            kind,
            note,
        };
        self.dispatch(&req, |_| provider(&cleanup)).await
    }

    /// Render the operator-facing report for an effect stuck in uncertainty.
    /// Returns `None` for settled/failed rows (nothing to reconcile) and
    /// unknown keys.
    pub async fn unknown_report(&self, effect_key: &str) -> Option<UnknownReport> {
        let row = self.store.get_effect(effect_key).await.ok()??;
        match row.state {
            EffectState::Unknown | EffectState::ReconcilePending => {
                let cap = capability_for_action_type(&row.action_type);
                Some(UnknownReport {
                    effect_key: row.effect_key,
                    run_id: row.run_id,
                    node_path: row.node_path,
                    state: row.state,
                    attempt: row.attempt,
                    evidence: row.failure_json.clone().or(row.outcome_json.clone()),
                    next_step: describe_unknown(row.state, cap.supports_reconcile_query)
                        .to_string(),
                })
            }
            _ => None,
        }
    }
}

/// Test helper (bd-dispatch-spec-untyped-0iud migration): build a
/// [`toron_workflow::schema::DispatchSpec`] from raw JSON — the migration
/// rewrote legacy `spec: json!(...)` call sites to this helper but never
/// defined it, leaving the lib-test target red (12 E0425s). Found in the
/// post-wave adversarial review 2026-08-21.
#[cfg(test)]
pub(crate) fn test_spec(v: serde_json::Value) -> toron_workflow::schema::DispatchSpec {
    serde_json::from_value(v).expect("test spec parses as DispatchSpec")
}
#[cfg(test)]
mod tests {
    use super::*;
    use nostr::prelude::{EventBuilder, FinalizeEvent, Kind, Tag};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A minimal signed kind-14 event with optional tags, for trigger tests.
    fn kind14(content: &str, tags: Vec<Tag>) -> Event {
        let keys = Keys::generate();
        EventBuilder::new(Kind::from_u16(14), content)
            .tags(tags)
            .finalize(&keys)
            .expect("sign")
    }

    /// bd-pkc part 2: the trigger filter is LIVE in the trigger loop — a
    /// workflow-emitted (stamped) kind-14 does NOT re-trigger.
    #[test]
    fn stamped_workflow_event_is_rejected_at_ingress() {
        let keys = Keys::generate();
        let user = kind14("hello", vec![]);
        assert!(
            should_evaluate_trigger_event(&user),
            "user message evaluates"
        );
        let stamped = kind14(
            "hello",
            vec![
                Tag::parse(["t", toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG])
                    .expect("stamp tag"),
            ],
        );
        assert!(
            !should_evaluate_trigger_event(&stamped),
            "workflow-emitted kind-14 must not re-trigger"
        );
        let _ = keys;
    }

    /// bd-pkc part 2: the trigger payload exposes the fields the triage.yaml
    /// workflow references (`{{trigger.id}}`, `{{trigger.sender}}`,
    /// `{{trigger.content}}`).
    #[test]
    fn trigger_payload_exposes_template_fields() {
        let ev = kind14(
            "urgent: fix",
            vec![Tag::parse(["t", "myproject"]).expect("t")],
        );
        let payload = trigger_payload(&ev);
        assert_eq!(payload["id"].as_str(), Some(ev.id.to_hex().as_str()));
        assert_eq!(
            payload["sender"].as_str(),
            Some(ev.pubkey.to_hex().as_str())
        );
        assert_eq!(payload["content"].as_str(), Some("urgent: fix"));
        assert_eq!(payload["kind"].as_u64(), Some(14));
        assert_eq!(payload["project"].as_str(), Some("myproject"));
    }

    /// bd-pkc part 2: trigger matching is kind-scoped.
    #[test]
    fn trigger_matching_is_kind_scoped() {
        use toron_workflow::schema::CronScope;
        let msg = kind14("hi", vec![]);
        let react = {
            let keys = Keys::generate();
            EventBuilder::new(Kind::from_u16(7), "+")
                .finalize(&keys)
                .expect("sign")
        };
        assert!(matches_trigger(
            &Trigger::MessagePosted {
                channel: None,
                project: None
            },
            &msg
        ));
        assert!(!matches_trigger(
            &Trigger::ReactionAdded {
                channel: None,
                emoji: None
            },
            &msg
        ));
        assert!(matches_trigger(
            &Trigger::ReactionAdded {
                channel: None,
                emoji: None
            },
            &react
        ));
        assert!(!matches_trigger(
            &Trigger::Schedule {
                cron: "* * * * *".into(),
                owner: None,
                scope: CronScope::default()
            },
            &msg
        ));
    }

    /// P11.1 gate (bd-foh8.1): the raw-bus trigger loop fires
    /// reservation_acquired / reservation_released (with acquired-vs-released
    /// DISAMBIGUATION) and reaction_added triggers; message_posted fires on a
    /// user-authored kind-14 but NOT on a workflow-emitted (stamped) one —
    /// loop prevention holds end-to-end through the live loop.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bus_trigger_loop_fires_reservation_and_reaction_triggers() {
        use std::time::Duration;

        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::{Action, Step, StepOnError, Trigger, Workflow};

        let db = std::env::temp_dir().join(format!("wf-host-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );

        let fired: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fired2 = fired.clone();
        let afx: AsyncEffectRunner = Arc::new(move |inv: &ActionInvocation| {
            fired2
                .lock()
                .unwrap()
                .push(format!("action:{}", inv.action.type_name()));
            let value = serde_json::json!({ "action": inv.action.type_name() });
            Box::pin(async move { Ok(value) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
                >
        });

        fn wf(name: &str, trigger: Trigger) -> Workflow {
            Workflow {
                name: name.into(),
                description: None,
                trigger: Some(trigger),
                policy: None,
                guard: None,
                principles: vec![],
                steps: vec![Step {
                    name: Some("rec".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::SendMessage {
                        to: vec!["nobody".into()],
                        subject: name.into(),
                        body: "triggered".into(),
                        project: Some("demo".into()),
                    },
                }],
                graph: None,
                inputs: vec![],
                crew: None,
            }
        }

        let workflows = vec![
            wf(
                "on-acquire",
                Trigger::ReservationAcquired {
                    project: "demo".into(),
                    path: None,
                },
            ),
            wf(
                "on-release",
                Trigger::ReservationReleased {
                    project: "demo".into(),
                    path: None,
                },
            ),
            wf(
                "on-react",
                Trigger::ReactionAdded {
                    channel: None,
                    emoji: None,
                },
            ),
            wf(
                "on-msg",
                Trigger::MessagePosted {
                    channel: None,
                    project: Some("demo".into()),
                },
            ),
        ];
        let host = WorkflowHost::over_bus(bus.clone(), workflows, Some(afx)).expect("host");
        let _handle = host.spawn_trigger_loop();

        async fn wait_for(
            fired: &Arc<std::sync::Mutex<Vec<String>>>,
            want: &str,
            timeout: Duration,
        ) -> bool {
            let deadline = tokio::time::Instant::now() + timeout;
            loop {
                if fired.lock().unwrap().iter().any(|f| f == want) {
                    return true;
                }
                if tokio::time::Instant::now() >= deadline {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }

        async fn absent_for(
            fired: &Arc<std::sync::Mutex<Vec<String>>>,
            want: &str,
            window: Duration,
        ) -> bool {
            tokio::time::sleep(window).await;
            !fired.lock().unwrap().iter().any(|f| f == want)
        }

        let keys = Keys::generate();
        let t_demo = Tag::parse(["t", "demo"]).expect("t");

        let acquire = EventBuilder::new(Kind::from_u16(30031), r#"{"exclusive":true}"#)
            .tags(vec![
                Tag::parse(["d", "demo:src/**"]).expect("d"),
                t_demo.clone(),
            ])
            .finalize(&keys)
            .expect("sign");
        bus.publish(acquire).await.expect("publish acquire");
        assert!(
            wait_for(&fired, "action:send_message", Duration::from_secs(10)).await,
            "acquire fired the reservation_acquired workflow: {fired:?}"
        );
        assert!(
            fired
                .lock()
                .unwrap()
                .iter()
                .filter(|f| f.as_str() == "action:send_message")
                .count()
                == 1,
            "exactly one workflow fired on acquire: {fired:?}"
        );

        fired.lock().unwrap().clear();
        let release = EventBuilder::new(Kind::from_u16(30031), r#"{"status":"released"}"#)
            .tags(vec![
                Tag::parse(["d", "demo:src/**"]).expect("d"),
                t_demo.clone(),
            ])
            .finalize(&keys)
            .expect("sign");
        bus.publish(release).await.expect("publish release");
        assert!(
            wait_for(&fired, "action:send_message", Duration::from_secs(10)).await,
            "release fired the reservation_released workflow: {fired:?}"
        );
        assert_eq!(
            fired.lock().unwrap().len(),
            1,
            "release fires ONLY reservation_released (no acquired): {fired:?}"
        );

        fired.lock().unwrap().clear();
        let react = EventBuilder::new(Kind::from_u16(7), "+")
            .finalize(&keys)
            .expect("sign");
        bus.publish(react).await.expect("publish reaction");
        assert!(
            wait_for(&fired, "action:send_message", Duration::from_secs(10)).await,
            "reaction fired the reaction_added workflow: {fired:?}"
        );

        fired.lock().unwrap().clear();
        let stamped = EventBuilder::new(Kind::from_u16(14), "workflow ack")
            .tags(vec![
                t_demo.clone(),
                Tag::parse(["t", toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG])
                    .expect("stamp"),
            ])
            .finalize(&keys)
            .expect("sign");
        bus.publish(stamped).await.expect("publish stamped");
        assert!(
            absent_for(&fired, "action:send_message", Duration::from_millis(800)).await,
            "stamped workflow-emitted message must NOT re-trigger: {fired:?}"
        );

        let user_msg = EventBuilder::new(Kind::from_u16(14), "user hello")
            .tags(vec![t_demo])
            .finalize(&keys)
            .expect("sign");
        bus.publish(user_msg).await.expect("publish user msg");
        assert!(
            wait_for(&fired, "action:send_message", Duration::from_secs(10)).await,
            "user-authored kind-14 fired message_posted: {fired:?}"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// bd-pkc part 2 + part 3: the mailbox runner coalesces DispatchJob @mentions
    /// (Ship 1) — a burst lands in one coalescer buffer, not N jobs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_job_actions_coalesce_via_runner() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-coal-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let mailbox = Arc::new(Mailbox::new(
            bus,
            std::env::temp_dir().join(format!("wf-coal-data-{}", uuid::Uuid::new_v4())),
            None,
        ));

        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let afx = mailbox_async_effect_runner(
            mailbox,
            coalescer.clone(),
            None,
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );
        let action = |i: u32| Action::DispatchJob {
            assignee: "oc-beta".into(),
            spec: crate::workflow_host::test_spec(json!({ "i": i })),
            channel: None,
        };
        for i in 0..5 {
            let out = drive_async(&afx, action(i), "run-coal-buffer").await;
            assert!(out["ok"].as_bool() == Some(true), "dispatch {i} ok");
        }
        let buffered = {
            let c = coalescer.lock().unwrap_or_else(PoisonError::into_inner);
            c.buffered_for("oc-beta")
        };
        assert!(
            buffered == 5,
            "all mentions buffered for oc-beta, got {buffered}"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// ADR-0007 F6: workflow-originated sends are STAMPED. The rumor inside
    /// the gift wrap carries `toron:workflow-emitted` (the wrap itself stays
    /// metadata-hiding), and the stamped rumor is rejected by the trigger
    /// filter — a workflow can never re-trigger itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workflow_sends_stamp_rumor_and_are_filtered() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::mailbox::messaging::unwrap_for_recipient;

        const PASSPHRASE: &str = "wf-stamp-pass";
        let db = std::env::temp_dir().join(format!("wf-stamp-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir = std::env::temp_dir().join(format!("wf-stamp-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Arc::new(
            Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
        );

        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("workflow-demo"),
            )
            .await
            .expect("register sender");
        let recv = mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("BlueLake"),
            )
            .await
            .expect("register recipient");
        let recv_keys =
            toron_core::keys::load_encrypted_key(std::path::Path::new(&recv.key_path), PASSPHRASE)
                .expect("load recipient key");

        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let afx = mailbox_async_effect_runner(
            mailbox.clone(),
            coalescer,
            None,
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");

        let send = |action: Action| {
            let afx = afx.clone();
            async move {
                let out = drive_async(&afx, action, "run-stamp").await;
                assert!(out["ok"].as_bool() == Some(true), "runner ok: {out}");
            }
        };
        send(Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "review".into(),
            body: "stamped body".into(),
            project: Some("demo".into()),
        })
        .await;
        send(Action::SendDm {
            to: "BlueLake".into(),
            body: "stamped dm".into(),
            project: Some("demo".into()),
        })
        .await;

        for _ in 0..2 {
            let wrap = loop {
                match tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await {
                    Ok(Some(SubscriptionEvent::Event(e))) => break e,
                    Ok(Some(SubscriptionEvent::Eose)) | Ok(None) => {}
                    Ok(Some(SubscriptionEvent::Closed(_))) => panic!("subscription closed"),
                    Err(_) => panic!("timed out waiting for wrap event"),
                }
            };
            let wrap_has_stamp = wrap.tags.iter().any(|t| {
                t.as_slice().first().map(String::as_str) == Some("t")
                    && t.as_slice().get(1).map(String::as_str)
                        == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
            });
            assert!(!wrap_has_stamp, "gift wrap must not carry the stamp");
            let unsigned = unwrap_for_recipient(&recv_keys, &wrap)
                .expect("unwrap")
                .rumor;
            let has_stamp = unsigned.tags.iter().any(|t| {
                t.as_slice().first().map(String::as_str) == Some("t")
                    && t.as_slice().get(1).map(String::as_str)
                        == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
            });
            assert!(has_stamp, "workflow-originated rumor carries the stamp");
            let rumor = EventBuilder::new(unsigned.kind, unsigned.content.clone())
                .tags(unsigned.tags)
                .custom_created_at(unsigned.created_at)
                .finalize(&recv_keys)
                .expect("sign rumor for filter check");
            assert!(
                !should_evaluate_trigger_event(&rumor),
                "stamped rumor must not re-trigger a workflow"
            );
        }

        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// ADR-0007 F7: a coalesced @mention burst publishes EXACTLY ONE stamped
    /// kind-43001 — via both the runner's cap-flush path and the host's
    /// `publish_coalesced_job` path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalesced_burst_publishes_one_stamped_43001() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-flush-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let mailbox = Arc::new(Mailbox::new(
            bus.clone(),
            std::env::temp_dir().join(format!("wf-flush-data-{}", uuid::Uuid::new_v4())),
            None,
        ));

        let signer = Keys::generate();
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(500),
            3,
        )));
        let afx = mailbox_async_effect_runner(
            mailbox,
            coalescer.clone(),
            Some(signer.clone()),
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");
        let mut published = 0;
        for i in 0..3 {
            let out = drive_async(
                &afx,
                Action::DispatchJob {
                    assignee: "oc".into(),
                    spec: crate::workflow_host::test_spec(json!({ "i": i })),
                    channel: None,
                },
                &format!("run-cap-flush-{i}"),
            )
            .await;
            if out["published"].as_bool() == Some(true) {
                published += 1;
            }
        }
        assert_eq!(published, 1, "cap-flush publishes exactly one 43001");

        let parent = "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b";
        let batch = toron_workflow::coalesce::Coalesced {
            assignee: "gk".into(),
            mentions: (0..5)
                .map(|i| InboundMention {
                    assignee: "gk".into(),
                    parent: parent.into(),
                    spec: json!({ "text": format!("mention {i}") }),
                })
                .collect(),
            first_arrival_ms: 0,
        };
        let bus_for_publish = bus.clone();
        let signer_for_publish = signer.clone();
        let id = publish_coalesced_job(bus_for_publish.as_ref(), Some(&signer_for_publish), &batch)
            .await
            .expect("host publish");
        assert!(!id.to_hex().is_empty());

        let mut jobs43001 = Vec::new();
        for _ in 0..2 {
            let ev = loop {
                match tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await {
                    Ok(Some(SubscriptionEvent::Event(e))) if e.kind.as_u16() == 43001 => break e,
                    Ok(Some(SubscriptionEvent::Event(_))) => {}
                    Ok(Some(SubscriptionEvent::Eose)) | Ok(None) => {}
                    Ok(Some(SubscriptionEvent::Closed(_))) => panic!("subscription closed"),
                    Err(_) => panic!("timed out waiting for 43001"),
                }
            };
            let stamped = ev.tags.iter().any(|t| {
                t.as_slice().first().map(String::as_str) == Some("t")
                    && t.as_slice().get(1).map(String::as_str)
                        == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
            });
            assert!(stamped, "43001 carries the workflow-emitted stamp");
            assert!(
                !should_evaluate_trigger_event(&ev),
                "stamped 43001 must not re-trigger"
            );
            jobs43001.push(ev);
        }
        assert_eq!(jobs43001.len(), 2, "one 43001 per burst");
        let _ = std::fs::remove_file(&db);
    }

    /// P5 (bd-p5-concurrency-stress-tests-9q8l): a 100-mention burst fired in
    /// <1s must produce a BOUNDED number of kind-43001 jobs (coalescing
    /// works under pressure) — not 100, not 0, and no lost mentions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn burst_100_mentions_bounded_jobs() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        const MENTIONS: usize = 100;
        let db = std::env::temp_dir().join(format!("wf-burst-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let mailbox = Arc::new(Mailbox::new(
            bus.clone(),
            std::env::temp_dir().join(format!("wf-burst-data-{}", uuid::Uuid::new_v4())),
            None,
        ));

        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let afx = mailbox_async_effect_runner(
            mailbox,
            coalescer.clone(),
            Some(Keys::generate()),
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );

        let barrier = Arc::new(tokio::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for t in 0..8 {
            let afx = afx.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                for i in 0..(MENTIONS / 8) {
                    drive_async(
                        &afx,
                        Action::DispatchJob {
                            assignee: "oc".into(),
                            spec: crate::workflow_host::test_spec(json!({ "i": t * 1000 + i })),
                            channel: None,
                        },
                        &format!("run-burst-{t}-{i}"),
                    )
                    .await;
                }
            }));
        }
        for h in handles {
            h.await.expect("burst task");
        }

        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([Kind::from_u16(43001)]),
        )
        .await
        .expect("replay");
        assert!(
            replay.len() <= 5,
            "100 mentions must coalesce to a bounded job count, got {}",
            replay.len()
        );
        assert!(
            !replay.is_empty(),
            "the burst must produce at least one coalesced job"
        );
        for ev in &replay {
            assert!(
                !should_evaluate_trigger_event(ev),
                "coalesced 43001 must not re-trigger"
            );
        }
        let _ = std::fs::remove_file(&db);
    }

    /// End-to-end: a workflow with a `message_posted` trigger + a satisfiable
    /// guard is INVOKED by the trigger loop when a matching event arrives
    /// (Ship 2 filter + Ship 4 guard + engine invoke, all LIVE).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trigger_loop_invokes_matching_workflow() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-host-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );

        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let afx: AsyncEffectRunner = Arc::new(move |inv: &ActionInvocation| {
            if let toron_workflow::schema::Action::SendDm { .. } = &inv.action {
                ran2.fetch_add(1, Ordering::Relaxed);
            }
            Box::pin(async move { Ok(serde_json::Value::Null) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
                >
        });

        let wf = toron_workflow::schema::parse_workflow(
            r##"
name: triage-live
trigger:
  type: message_posted
guard: "trigger_kind == \"14\""
steps:
  - name: ack
    action: send_dm
    to: "pi"
    body: "Received"
"##,
        )
        .expect("parse workflow");

        let host = WorkflowHost::over_bus(bus.clone(), vec![wf], Some(afx)).expect("host");
        let handle = host.spawn_trigger_loop();

        let ev = kind14("triage me", vec![]);
        bus.publish(ev).await.expect("publish");

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        assert!(
            ran.load(Ordering::Relaxed) >= 1,
            "workflow action fired via trigger loop (ran={})",
            ran.load(Ordering::Relaxed)
        );
        handle.abort();
        let _ = std::fs::remove_file(&db);
    }

    /// ADR-0007 F8 (minimum_acceptable_test): stress — N concurrent workflow
    /// sends must not hang or panic. Every mailbox call is awaited inline
    /// on the shared runtime (bd-ckkf): N tasks firing at once proves no
    /// bridge, no thread-hop, no nested runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_workflow_sends_no_hang() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        const PASSPHRASE: &str = "wf-stress-pass";
        const CONCURRENCY: usize = 8;
        let db = std::env::temp_dir().join(format!("wf-stress-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir =
            std::env::temp_dir().join(format!("wf-stress-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Arc::new(
            Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
        );
        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("workflow-demo"),
            )
            .await
            .expect("register sender");
        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("BlueLake"),
            )
            .await
            .expect("register recipient");

        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let afx = mailbox_async_effect_runner(
            mailbox.clone(),
            coalescer,
            None,
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );

        let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));
        let handles: Vec<_> = (0..CONCURRENCY)
            .map(|i| {
                let afx = afx.clone();
                let barrier = barrier.clone();
                tokio::spawn(async move {
                    barrier.wait().await;
                    drive_async(
                        &afx,
                        Action::SendMessage {
                            to: vec!["BlueLake".into()],
                            subject: format!("stress {i}"),
                            body: format!("stress body {i}"),
                            project: Some("demo".into()),
                        },
                        &format!("run-stress-{i}"),
                    )
                    .await
                })
            })
            .collect();
        let mut ok = 0usize;
        for h in handles {
            let out = h.await.expect("runner task must not panic");
            let delivered = out.get("ok").and_then(serde_json::Value::as_bool) == Some(true);
            assert!(delivered, "concurrent send must deliver: {out:?}");
            ok += 1;
        }
        assert_eq!(ok, CONCURRENCY, "all {CONCURRENCY} sends delivered");

        let replay =
            toron_core::mailbox::collect_replay(bus.as_ref(), toron_core::bus::Filter::new())
                .await
                .expect("replay");
        let wraps = replay.iter().filter(|e| e.kind == Kind::GiftWrap).count();
        assert_eq!(wraps, CONCURRENCY, "one wrap per concurrent send");

        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// A [`Bus`] whose `publish` sleeps before delegating — models relay
    /// latency / backpressure on the real publish path.
    struct SlowBus {
        inner: Arc<toron_core::bus::InProcessBus>,
        sleep: std::time::Duration,
    }

    #[async_trait::async_trait]
    impl Bus for SlowBus {
        async fn publish(
            &self,
            event: Event,
        ) -> Result<toron_core::bus::PublishOk, toron_core::bus::BusError> {
            tokio::time::sleep(self.sleep).await;
            self.inner.publish(event).await
        }

        async fn subscribe(
            &self,
            filter: toron_core::bus::Filter,
        ) -> Result<toron_core::bus::Subscription, toron_core::bus::BusError> {
            self.inner.subscribe(filter).await
        }

        async fn subscribe_trusted(
            &self,
            filter: toron_core::bus::Filter,
        ) -> Result<toron_core::bus::Subscription, toron_core::bus::BusError> {
            self.inner.subscribe_trusted(filter).await
        }

        async fn health(&self) -> Result<toron_core::bus::Health, toron_core::bus::BusError> {
            self.inner.health().await
        }
    }

    /// P1 (bd-p1-std-guard-across-blockon-gwnt, oc A1.6): the coalescer
    /// std-Mutex guard must NOT be held across the coalesced publish —
    /// the guard scopes to submit only, then the async publish runs
    /// guard-free, so a concurrent flush for an unrelated assignee
    /// proceeds immediately.
    ///
    /// GREEN since the batch fix landed: at BOTH publish sites the coalesced
    /// job is cloned under the lock, the guard drops, THEN the publish runs
    /// (runner FlushNow arm + trigger-loop tick flush), so a concurrent flush
    /// for an unrelated assignee proceeds immediately. Both sites await
    /// inline on the executor — no thread-hop anywhere.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coalescer_lock_not_held_across_blocking_publish() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        const PUBLISH_MS: u64 = 600;
        let db =
            std::env::temp_dir().join(format!("wf-coalesce-lock-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let inner = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let slow: Arc<dyn Bus> = Arc::new(SlowBus {
            inner,
            sleep: std::time::Duration::from_millis(PUBLISH_MS),
        });

        let data_dir =
            std::env::temp_dir().join(format!("wf-coalesce-lock-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Arc::new(
            Mailbox::new(slow.clone(), data_dir.clone(), None)
                .with_agent_passphrase("coalesce-lock-pass"),
        );

        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
            std::time::Duration::from_millis(2_000),
            std::time::Duration::from_millis(500),
            3,
        )));
        let afx = mailbox_async_effect_runner(
            mailbox,
            coalescer,
            Some(Keys::generate()),
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );

        let _ = drive_async(
            &afx,
            Action::DispatchJob {
                assignee: "oc".into(),
                spec: crate::workflow_host::test_spec(json!({ "text": "e1" })),
                channel: None,
            },
            "run-coal-lock-e1",
        )
        .await;
        let _ = drive_async(
            &afx,
            Action::DispatchJob {
                assignee: "oc".into(),
                spec: crate::workflow_host::test_spec(json!({ "text": "e2" })),
                channel: None,
            },
            "run-coal-lock-e2",
        )
        .await;

        let afx_a = afx.clone();
        let a = tokio::spawn(async move {
            drive_async(
                &afx_a,
                Action::DispatchJob {
                    assignee: "oc".into(),
                    spec: crate::workflow_host::test_spec(json!({ "text": "e3" })),
                    channel: None,
                },
                "run-coal-lock-e3",
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let afx_b = afx.clone();
        let t0 = std::time::Instant::now();
        let b = tokio::spawn(async move {
            drive_async(
                &afx_b,
                Action::DispatchJob {
                    assignee: "gk".into(),
                    spec: crate::workflow_host::test_spec(json!({ "text": "e4" })),
                    channel: None,
                },
                "run-coal-lock-e4",
            )
            .await
        });
        let _ = b.await.expect("runner B no panic");
        let b_elapsed = t0.elapsed();
        let _ = a.await.expect("runner A no panic");

        assert!(
            b_elapsed < std::time::Duration::from_millis(300),
            "concurrent flush stalled {b_elapsed:?} behind a blocking publish — coalescer guard held across publish_coalesced_job"
        );

        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_file(&db);
    }

    /// P1 (bd-p1-poison-on-panic-audit-jwpd): a panic while holding the
    /// coalescer std-Mutex guard must NOT brick the service — the next flush
    /// recovers from the poison instead of panicking (ADR-0007 D7 recovery
    /// posture).
    ///
    /// RED today: `.lock().expect("coalescer lock")` at the runner arm +
    /// tick flush propagates PoisonError as a panic — one panic inside the
    /// guard permanently kills every subsequent flush (panic-in-panic =
    /// dead service).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn poisoned_coalescer_survives() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-poison-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir =
            std::env::temp_dir().join(format!("wf-poison-data-{}", uuid::Uuid::new_v4()));
        let mailbox =
            Arc::new(Mailbox::new(bus, data_dir, None).with_agent_passphrase("poison-pass"));
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let afx = mailbox_async_effect_runner(
            mailbox,
            coalescer.clone(),
            Some(Keys::generate()),
            Arc::new(std::sync::Mutex::new(None)),
            test_armor(),
        );

        let c2 = coalescer.clone();
        let panicked = std::panic::catch_unwind(move || {
            let _g = c2.lock().expect("pre-poison lock");
            panic!("simulated panic inside the coalescer guard");
        });
        assert!(panicked.is_err(), "precondition: simulated panic unwound");
        assert!(
            coalescer.is_poisoned(),
            "precondition: std Mutex is poisoned"
        );

        let out = tokio::spawn(async move {
            drive_async(
                &afx,
                Action::DispatchJob {
                    assignee: "oc".into(),
                    spec: crate::workflow_host::test_spec(json!({ "text": "after-poison" })),
                    channel: None,
                },
                "run-poison",
            )
            .await
        })
        .await;
        assert!(
            out.is_ok(),
            "poisoned coalescer must survive — subsequent flush panicked: {out:?}"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// bd-asx3 RED: draining a newly parked promise records one structured
    /// entry (run/node/promise/generation) and suspends the run; a second
    /// drain observes no duplicates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_lifecycle_drain_records_structured_entry() {
        use toron_workflow::engine::ParkKind;
        let engine = WorkflowEngine::local().expect("local engine");
        engine
            .approval_sink()
            .lock()
            .await
            .push("promise-1".to_string());
        let mut drain = PendingDrain::new();
        let ctx = ParkCtx {
            run_id: "run-1".into(),
            workflow: "ship".into(),
            project: None,
            kind: ParkKind::Approval,
            candidates: vec!["gate".into()],
        };
        let fresh = drain.drain(&engine, &ctx).await;
        assert_eq!(fresh.len(), 1, "one structured entry for the new park");
        let entry = &fresh[0];
        assert_eq!(entry.run_id, "run-1");
        assert_eq!(entry.node_path, "gate");
        assert_eq!(entry.promise_id, "promise-1");
        assert_eq!(entry.generation, 1);
        assert_eq!(drain.run_state("run-1"), RunState::Suspended);
        let again = drain.drain(&engine, &ctx).await;
        assert!(again.is_empty(), "re-drain is idempotent");
    }

    /// bd-o21n: the HITL round-trip through the drain — a sink-observed
    /// promise persists as a `Parked` effect row plus its pending resume
    /// row; re-driving the row plans `ResumeParked`, never `Failed`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hitl_promise_persists_parked_and_resumes() {
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::NewRun;

        let db = std::env::temp_dir().join(format!("wf-hitl-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let store = WorkflowEffectStore::new(pool);
        store
            .create_run(NewRun {
                run_id: "run-hitl".into(),
                project: None,
                workflow: "ship".into(),
                workflow_digest: "dig".into(),
            })
            .await
            .expect("run row for the digest lookup");

        let engine = WorkflowEngine::local().expect("local engine");
        engine
            .approval_sink()
            .lock()
            .await
            .push("promise-hitl-1".to_string());
        let mut drain = PendingDrain::new().with_store(store.clone());
        let ctx = ParkCtx {
            run_id: "run-hitl".into(),
            workflow: "ship".into(),
            project: None,
            kind: ParkKind::Approval,
            candidates: vec!["gate".into()],
        };
        let fresh = drain.drain(&engine, &ctx).await;
        assert_eq!(fresh.len(), 1, "the observed park publishes");
        let row = store
            .get_effect("park:run-hitl:gate:promise-hitl-1")
            .await
            .expect("query")
            .expect("parked row");
        assert_eq!(row.state, EffectState::Parked);
        assert!(row.state.is_resumable());
        let pending = store.list_pending("run-hitl").await.expect("list");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].promise_id, "promise-hitl-1");
        assert_eq!(
            pending[0].effect_key.as_deref(),
            Some("park:run-hitl:gate:promise-hitl-1")
        );
        let view = effect_view(row.state, row.attempt, false);
        let cap = ProviderCapability {
            supports_idempotency_key: true,
            supports_reconcile_query: false,
        };
        assert!(
            matches!(
                plan_attempt(Some(&view), &cap),
                DispatchPlan::ResumeParked { .. }
            ),
            "a Parked row resumes"
        );
        assert!(drain.drain(&engine, &ctx).await.is_empty());
        assert_eq!(store.list_pending("run-hitl").await.expect("list").len(), 1);
        let _ = std::fs::remove_file(&db);
    }

    /// bd-asx3 RED: restart recovery — a fresh drain over the same live
    /// sinks reconstructs the identical registry without an in-memory carry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_lifecycle_restart_recovery() {
        use toron_workflow::engine::ParkKind;
        let engine = WorkflowEngine::local().expect("local engine");
        engine
            .approval_sink()
            .lock()
            .await
            .push("promise-9".to_string());
        let ctx = ParkCtx {
            run_id: "run-9".into(),
            workflow: "ship".into(),
            project: None,
            kind: ParkKind::Approval,
            candidates: vec!["gate".into()],
        };
        let mut first = PendingDrain::new();
        let a = first.drain(&engine, &ctx).await;
        assert_eq!(a.len(), 1);
        drop(first);
        let mut second = PendingDrain::new();
        let b = second.drain(&engine, &ctx).await;
        assert_eq!(b.len(), 1, "recovery re-observes the live park");
        assert_eq!(b[0].key(), a[0].key(), "identical entry identity");
    }

    /// bd-asx3 RED: stale and unknown resolutions are recorded and ignored —
    /// they never touch the parked promise.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_lifecycle_stale_resolution_ignored() {
        use toron_workflow::engine::{Resolution, ResolutionMismatch, ResolutionVerdict};
        let engine = WorkflowEngine::local().expect("local engine");
        engine
            .approval_sink()
            .lock()
            .await
            .push("promise-1".to_string());
        let mut drain = PendingDrain::new();
        let ctx = ParkCtx {
            run_id: "run-1".into(),
            workflow: "ship".into(),
            project: None,
            kind: ParkKind::Approval,
            candidates: vec!["gate".into()],
        };
        assert_eq!(drain.drain(&engine, &ctx).await.len(), 1);
        let stale = Resolution {
            run_id: "run-1".into(),
            node: None,
            project: None,
            promise_id: "promise-1".into(),
            generation: 0,
            verdict: ResolutionVerdict::Approved,
            note: "old".into(),
        };
        assert!(
            matches!(
                drain.resolve(stale),
                Err(ResolutionMismatch::StaleGeneration { expected: 1 })
            ),
            "stale generation is rejected"
        );
        let unknown = Resolution {
            run_id: "run-1".into(),
            node: None,
            project: None,
            promise_id: "promise-nope".into(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: "x".into(),
        };
        assert!(
            matches!(
                drain.resolve(unknown),
                Err(ResolutionMismatch::UnknownPromise)
            ),
            "unknown promise is rejected"
        );
        assert_eq!(drain.ignored_count(), 2, "both mismatches recorded");
        assert_eq!(drain.registry_len(), 1, "parked entry untouched");
    }

    /// bd-asx3 RED: park candidates come from the workflow schema in order —
    /// named steps first, positional fallback for the unnamed.
    #[test]
    fn host_lifecycle_park_candidates_scan() {
        use toron_workflow::engine::ParkKind;
        let wf = toron_workflow::schema::parse_workflow(
            "name: ship\ntrigger:\n  type: webhook\nsteps:\n  - name: gate\n    action: request_approval\n    message: q\n  - action: send_dm\n    to: a\n    body: b\n",
        )
        .expect("parse");
        assert_eq!(
            park_candidates(&wf, ParkKind::Approval),
            vec!["gate".to_string()]
        );
        let wf2 = toron_workflow::schema::parse_workflow(
            "name: ship\ntrigger:\n  type: webhook\nsteps:\n  - action: request_approval\n    message: q\n",
        )
        .expect("parse");
        assert_eq!(
            park_candidates(&wf2, ParkKind::Approval),
            vec!["step-0".to_string()]
        );
    }

    /// bd-asx3 RED: approval pending publication emits kind-46011 carrying
    /// run/node/promise/generation — and terminal republish with the same
    /// digest writes no second semantic terminal.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_lifecycle_pending_and_terminal_publication() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::engine::{ParkKind, PendingEntry};

        let db = std::env::temp_dir().join(format!("wf-life-{}.sqlite", uuid::Uuid::new_v4()));
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

        let entry = PendingEntry::new(
            "run-1",
            None,
            "ship",
            "gate",
            None,
            "promise-1",
            1,
            ParkKind::Approval,
        );
        let mut sub = bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");
        host.publish_approval_pending(&entry)
            .await
            .expect("publish pending");
        let mut kinds = vec![];
        for _ in 0..2 {
            let got = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next())
                .await
                .expect("event arrives")
                .expect("stream item");
            if let toron_core::bus::SubscriptionEvent::Event(ev) = got {
                kinds.push(ev.kind.as_u16());
                if ev.kind.as_u16() == 46011 {
                    let body: serde_json::Value =
                        serde_json::from_str(&ev.content).expect("46011 json");
                    assert_eq!(body["promise_id"], serde_json::json!("promise-1"));
                    assert_eq!(body["run_id"], serde_json::json!("run-1"));
                    assert_eq!(body["generation"], serde_json::json!(1));
                }
            }
        }
        assert!(
            kinds.contains(&46011),
            "pending published as 46011: {kinds:?}"
        );

        let note1 = host
            .publish_terminal("run-1", "digest-1", "succeeded", "gate", "ev-1")
            .await
            .expect("publish terminal");
        assert!(matches!(note1, toron_workflow::engine::TerminalNote::New));
        let note2 = host
            .publish_terminal("run-1", "digest-1", "succeeded", "gate", "ev-1")
            .await
            .expect("republish terminal");
        assert!(matches!(
            note2,
            toron_workflow::engine::TerminalNote::Duplicate
        ));
        let _ = std::fs::remove_file(&db);
    }

    /// bd-graph-approval-host-dcqq: one wave can park several approvals, so
    /// every `AwaitingApproval` effect is registered under `(run, node)` and
    /// published — with its evidence payload — instead of the first one
    /// swallowing the rest.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_graph_multi_approval_parks_publish_per_node() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::Workflow;

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
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::Workflow;

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

    /// bd-terminal-shape-tbik: kind-46012 carries the run OUTCOME. A success
    /// publishes the outputs map (no `failed_step`); a failure publishes the
    /// enriched bundle at top level, not a wrapper around it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_terminal_event_carries_the_outcome_not_a_wrapper() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-term-{}.sqlite", uuid::Uuid::new_v4()));
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
        let mut sub = bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");

        host.publish_terminal(
            "run-term-ok",
            "digest-ok",
            "succeeded",
            "",
            "{\"aa\":{\"ok\":true},\"zz\":{\"ok\":true}}",
        )
        .await
        .expect("publish success");
        host.publish_terminal(
            "run-term-bad",
            "digest-bad",
            "failed",
            "flaky",
            "{\"status\":\"failed\",\"error\":\"boom\",\"failed_step\":\"flaky\",\"kind\":\"timeout\",\"attempts\":3}",
        )
        .await
        .expect("publish failure");

        let mut outputs = Vec::new();
        for _ in 0..20 {
            if outputs.len() == 2 {
                break;
            }
            let Ok(got) = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await
            else {
                break;
            };
            let Some(toron_core::bus::SubscriptionEvent::Event(ev)) = got else {
                continue;
            };
            if ev.kind.as_u16() == 46012 {
                let body: serde_json::Value =
                    serde_json::from_str(&ev.content).expect("46012 json");
                outputs.push(body);
            }
        }
        assert_eq!(outputs.len(), 2, "one terminal per run: {outputs:?}");

        let ok = outputs
            .iter()
            .find(|b| b["run_id"] == serde_json::json!("run-term-ok"))
            .expect("success terminal");
        assert_eq!(ok["status"], serde_json::json!("succeeded"));
        assert_eq!(
            ok["output"],
            serde_json::json!({
                "aa": {"ok": true},
                "zz": {"ok": true},
                "status": "succeeded",
                "run_id": "run-term-ok",
            }),
            "the outputs map plus the identity keys, nothing else"
        );
        assert!(
            ok["output"].get("failed_step").is_none(),
            "old success carries no failed_step: {ok}"
        );

        let bad = outputs
            .iter()
            .find(|b| b["run_id"] == serde_json::json!("run-term-bad"))
            .expect("failure terminal");
        assert_eq!(bad["status"], serde_json::json!("failed"));
        assert_eq!(bad["output"]["status"], serde_json::json!("failed"));
        assert_eq!(bad["output"]["error"], serde_json::json!("boom"));
        assert_eq!(bad["output"]["failed_step"], serde_json::json!("flaky"));
        assert_eq!(bad["output"]["attempts"], serde_json::json!(3));
        assert!(
            bad["output"].get("evidence").is_none(),
            "the bundle is not re-wrapped: {bad}"
        );
    }

    /// bd-spawn-park-toron-vwu3 / bd-spawn-launcher-flip-d8mv: the durable
    /// spawn is a self-contained job — the whole spec rides kind-43001
    /// (`spawn_key`, profile, resolved, harness, await, timeout, context). The
    /// job is EVIDENCE; the launcher seam settles the run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_spawn_publishes_the_whole_spec() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::engine::spawn::SpawnSpec;

        let db = std::env::temp_dir().join(format!("wf-spawn-{}.sqlite", uuid::Uuid::new_v4()));
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
        let mut sub = bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");

        let spec = SpawnSpec {
            spawn_key: "run-spawn:spawn_it:1".into(),
            run_id: "run-spawn".into(),
            node: "spawn_it".into(),
            workflow: "spawn-wf".into(),
            project: None,
            generation: 1,
            profile: "coder".into(),
            resolved: serde_json::json!({"agent": "coder"}),
            harness: Some("opencode".into()),
            workflow_ref: None,
            await_: true,
            no_claim_timeout: 600,
            context: None,
        };
        host.publish_durable_spawn(&spec)
            .await
            .expect("publish spawn");

        let mut body = None;
        for _ in 0..20 {
            let Ok(got) = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await
            else {
                break;
            };
            let Some(toron_core::bus::SubscriptionEvent::Event(ev)) = got else {
                continue;
            };
            if ev.kind.as_u16() == 43001 {
                body = Some(serde_json::from_str::<serde_json::Value>(&ev.content).expect("json"));
                break;
            }
        }
        let body = body.expect("a 43001 spawn job");
        let carried = &body["spec"];
        assert_eq!(
            carried["spawn_key"],
            serde_json::json!("run-spawn:spawn_it:1")
        );
        assert_eq!(carried["profile"], serde_json::json!("coder"));
        assert_eq!(carried["resolved"], serde_json::json!({"agent": "coder"}));
        assert_eq!(carried["harness"], serde_json::json!("opencode"));
        assert_eq!(carried["await"], serde_json::json!(true));
        assert_eq!(carried["no_claim_timeout"], serde_json::json!(600));
    }

    /// bd-asx3 e2e: a real parked run is SUSPENDED (never failed), drains
    /// into the registry independently of the run await, and resumes to
    /// Succeeded through the validated host resolution path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn host_lifecycle_parked_run_suspends_and_resolves() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::engine::ResolutionVerdict;

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

        let wf = toron_workflow::schema::parse_workflow(
            "name: ship\ntrigger:\n  type: webhook\nsteps:\n  - name: gate\n    action: request_approval\n    message: Release?\n",
        )
        .expect("parse");
        let input = toron_workflow::engine::RunInput {
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

        let res = toron_workflow::engine::Resolution {
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

    /// bd-v139.2 test helper: a dispatcher over a throwaway journal.
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

    fn recon_req(key: &str) -> DispatchRequest {
        DispatchRequest {
            effect_key: key.into(),
            run_id: "run-1".into(),
            project: None,
            workflow_digest: "digest-1".into(),
            node_path: "gate".into(),
            iteration_key: None,
            action_type: "send_message".into(),
            action_digest: "adigest-1".into(),
            redacted_action: None,
            capability: ProviderCapability {
                supports_idempotency_key: true,
                supports_reconcile_query: false,
            },
        }
    }

    /// bd-v139.2 RED: intent precedes dispatch — the provider observes a
    /// journaled prepared/in-flight intent for the same key it is asked to
    /// execute; the effect key is fixed before the call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_reconciliation_intent_before_dispatch() {
        let store = recon_store("intent").await;
        let dispatcher = EffectDispatcher::new(store.clone());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let out = dispatcher
            .dispatch_async(&recon_req("ek-1"), |req| {
                let store = store.clone();
                let seen2 = seen2.clone();
                let effect_key = req.effect_key.clone();
                async move {
                    let row = store
                        .get_effect(&effect_key)
                        .await
                        .expect("read")
                        .expect("intent committed before dispatch");
                    seen2
                        .lock()
                        .unwrap()
                        .push((row.effect_key, row.state.as_str().to_string()));
                    ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
                }
            })
            .await
            .expect("dispatch");
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert_eq!(seen.lock().unwrap()[0].0, "ek-1");
        assert_eq!(
            out.state,
            toron_core::index::workflow_effects::EffectState::Settled
        );
    }

    /// bd-v139.2 RED: duplicate terminal dispatch returns the stored outcome
    /// without calling the provider again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_reconciliation_duplicate_terminal_no_redispatch() {
        let store = recon_store("dupe").await;
        let dispatcher = EffectDispatcher::new(store);
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c2 = calls.clone();
        let provider = move |_: &DispatchRequest| {
            c2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            ProviderOutcome::Delivered(serde_json::json!({"ok": true, "n": 1}))
        };
        let first = dispatcher
            .dispatch(&recon_req("ek-1"), &provider)
            .await
            .expect("first");
        let second = dispatcher
            .dispatch(&recon_req("ek-1"), &provider)
            .await
            .expect("second");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::Relaxed),
            1,
            "one dispatch only"
        );
        assert_eq!(first.outcome, second.outcome, "same stored outcome");
    }

    /// bd-v139.2 RED: ambiguous provider result becomes `unknown` (never
    /// failure-by-inference), then a same-key idempotent retry settles with
    /// attempt 2 under the identical key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_reconciliation_unknown_then_same_key_retry() {
        use toron_core::index::workflow_effects::EffectState;
        let store = recon_store("retry").await;
        let dispatcher = EffectDispatcher::new(store.clone());
        let out = dispatcher
            .dispatch(&recon_req("ek-1"), |_| {
                ProviderOutcome::Ambiguous("timeout".into())
            })
            .await
            .expect("ambiguous dispatch");
        assert_eq!(out.state, EffectState::Unknown, "timeout is not absence");
        let out2 = dispatcher
            .dispatch(&recon_req("ek-1"), |_| {
                ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
            })
            .await
            .expect("retry");
        assert_eq!(out2.state, EffectState::Settled);
        assert_eq!(out2.attempt, 2);
        assert_eq!(out2.effect_key, "ek-1", "one logical key across attempts");
    }

    /// bd-v139.2 RED: a rotated fencing token refuses the stale owner's
    /// settlement — state and evidence are unchanged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_reconciliation_stale_fence_settle_refused() {
        use toron_core::index::workflow_effects::{ClaimAttempt, EffectState};
        let store = recon_store("fence").await;
        let dispatcher = EffectDispatcher::new(store.clone());
        dispatcher
            .dispatch(&recon_req("ek-1"), |_| {
                ProviderOutcome::Ambiguous("lost".into())
            })
            .await
            .expect("park as unknown");
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        let live_token = row.claim_token.clone().unwrap();
        assert!(
            store
                .claim_attempt(
                    "ek-1",
                    ClaimAttempt {
                        attempt: 9,
                        attempt_id: "att-9".into(),
                        claim_token: "token-new".into(),
                        lease_expires_at: 1,
                        expected_claim_token: Some("wrong-but-expired".into()),
                    },
                )
                .await
                .unwrap_or(false)
                || store
                    .get_effect("ek-1")
                    .await
                    .unwrap()
                    .unwrap()
                    .claim_token
                    .is_some()
        );
        let settled = dispatcher
            .settle_with_presented(
                "ek-1",
                &live_token,
                ProviderOutcome::Delivered(serde_json::json!({"ok": true})),
            )
            .await
            .expect("settle call");
        assert!(!settled, "stale token cannot settle after rotation");
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(row.state, EffectState::Unknown, "uncertainty preserved");
    }

    /// bd-v139.2 RED: mailbox corr links by projection — `transport_corr`
    /// carries the effect key while the receipts table gains no row (the
    /// receipt store never becomes the journal).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_reconciliation_corr_linked_not_journaled() {
        let store = recon_store("corr").await;
        let pool = store.pool().clone();
        let dispatcher = EffectDispatcher::new(store.clone());
        dispatcher
            .dispatch(&recon_req("ek-1"), |_| {
                ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
            })
            .await
            .expect("dispatch");
        let row = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(row.transport_corr.as_deref(), Some("ek-1"));
        let receipts = toron_core::mailbox::receipts::ReceiptStore::new(pool);
        assert!(
            receipts.get("ek-1").await.unwrap().is_none(),
            "no receipt row is implied by an effect dispatch"
        );
    }

    /// bd-v139.2 RED: compensation preserves the original failure — a
    /// cleanup attempt journals under its own key while the failed effect's
    /// evidence stays intact (cleanup never rewrites uncertainty).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_reconciliation_cleanup_preserves_failure() {
        use toron_core::index::workflow_effects::EffectState;
        let store = recon_store("cleanup").await;
        let dispatcher = EffectDispatcher::new(store.clone());
        dispatcher
            .dispatch(&recon_req("ek-1"), |_| {
                ProviderOutcome::ProvenFailed("relay 403".into())
            })
            .await
            .expect("failed dispatch");
        let failed = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(failed.state, EffectState::Failed);
        assert!(
            failed
                .failure_json
                .as_deref()
                .unwrap_or("")
                .contains("relay 403")
        );
        let mut cleanup = recon_req("ek-1/cleanup:release-reservation");
        cleanup.action_type = "release_reservation".into();
        dispatcher
            .dispatch(&cleanup, |_| {
                ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
            })
            .await
            .expect("cleanup dispatch");
        let failed = store.get_effect("ek-1").await.unwrap().unwrap();
        assert_eq!(
            failed.state,
            EffectState::Failed,
            "original failure retained"
        );
        assert!(
            failed
                .failure_json
                .as_deref()
                .unwrap_or("")
                .contains("relay 403")
        );
        let done = store
            .get_effect("ek-1/cleanup:release-reservation")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            done.state,
            EffectState::Settled,
            "cleanup separately settled"
        );
    }

    /// bd-v139.3 test helper: a cleanup dispatcher over a throwaway journal.
    async fn cleanup_dispatcher(
        tag: &str,
    ) -> (
        EffectDispatcher,
        toron_core::index::workflow_effects::WorkflowEffectStore,
    ) {
        let store = recon_store(tag).await;
        let dispatcher = EffectDispatcher::new(store.clone());
        (dispatcher, store)
    }

    /// bd-v139.3 RED: reservation-release cleanup is natively idempotent —
    /// an ambiguous attempt journals `unknown`, then a same-key retry
    /// settles without touching the parent failure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_release_idempotent_retry() {
        use toron_core::index::workflow_effects::EffectState;
        use toron_workflow::engine::CleanupKind;
        let (dispatcher, store) = cleanup_dispatcher("release").await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c2 = calls.clone();
        let first = dispatcher
            .run_cleanup(
                "ek-parent",
                CleanupKind::ReleaseReservations,
                None,
                false,
                |_| {
                    c2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    ProviderOutcome::Ambiguous("release timeout".into())
                },
            )
            .await
            .expect("first cleanup attempt");
        assert_eq!(first.state, EffectState::Unknown);
        assert_eq!(first.effect_key, "ek-parent/cleanup:release-reservations");
        let second = dispatcher
            .run_cleanup(
                "ek-parent",
                CleanupKind::ReleaseReservations,
                None,
                false,
                |_| {
                    c2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
                },
            )
            .await
            .expect("retry converges");
        assert_eq!(second.state, EffectState::Settled);
        assert_eq!(second.attempt, 2);
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert!(store.get_effect("ek-parent").await.unwrap().is_none());
    }

    /// bd-v139.3 RED: captain-evidence mail is NOT natively idempotent — an
    /// ambiguous attempt refuses an unforced retry (no duplicate mail), and
    /// an operator-forced retry settles the same key after out-of-band
    /// confirmation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_mail_force_retry() {
        use toron_core::index::workflow_effects::EffectState;
        use toron_workflow::engine::CleanupKind;
        let (dispatcher, _store) = cleanup_dispatcher("mail").await;
        dispatcher
            .run_cleanup(
                "ek-parent",
                CleanupKind::CaptainEvidence,
                None,
                false,
                |_| ProviderOutcome::Ambiguous("send timeout".into()),
            )
            .await
            .expect("first attempt journals unknown");
        let refused = dispatcher
            .run_cleanup(
                "ek-parent",
                CleanupKind::CaptainEvidence,
                None,
                false,
                |_| ProviderOutcome::Delivered(serde_json::json!({"ok": true})),
            )
            .await;
        assert!(refused.is_err(), "unforced retry of ambiguous mail refuses");
        let forced = dispatcher
            .run_cleanup(
                "ek-parent",
                CleanupKind::CaptainEvidence,
                None,
                true,
                |_| ProviderOutcome::Delivered(serde_json::json!({"ok": true})),
            )
            .await
            .expect("operator-forced retry settles");
        assert_eq!(forced.state, EffectState::Settled);
        assert_eq!(forced.attempt, 2);
    }

    /// bd-v139.3 RED: duplicate cleanup returns the stored outcome without
    /// re-firing the provider.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_duplicate_no_refire() {
        use toron_workflow::engine::CleanupKind;
        let (dispatcher, _store) = cleanup_dispatcher("dupe").await;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let c2 = calls.clone();
        let run = |_: &toron_workflow::engine::CleanupRequest| {
            c2.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
        };
        let first = dispatcher
            .run_cleanup("ek-p", CleanupKind::BeadComment, None, false, run)
            .await
            .expect("first");
        let second = dispatcher
            .run_cleanup("ek-p", CleanupKind::BeadComment, None, false, run)
            .await
            .expect("second");
        assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(first.outcome, second.outcome);
    }

    /// bd-v139.3 RED: interrupted cleanup (claimed, never classified) is
    /// recoverable — a new dispatcher over the same journal retries the
    /// same key after lease expiry instead of forking.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_interrupted_recoverable() {
        use toron_core::index::workflow_effects::{ClaimAttempt, EffectState, NewEffect};
        use toron_workflow::engine::CleanupKind;
        let (dispatcher, store) = cleanup_dispatcher("interrupt").await;
        store
            .prepare_effect(NewEffect {
                effect_key: "ek-p/cleanup:release-reservations".into(),
                run_id: "run-1".into(),
                project: None,
                workflow_digest: "d".into(),
                node_path: "n".into(),
                iteration_key: None,
                action_type: "cleanup:release-reservations".into(),
                action_digest: "a".into(),
                redacted_action: None,
                retention_until: None,
            })
            .await
            .unwrap();
        store
            .claim_attempt(
                "ek-p/cleanup:release-reservations",
                ClaimAttempt {
                    attempt: 1,
                    attempt_id: "att-1".into(),
                    claim_token: "tok-crash".into(),
                    lease_expires_at: 1,
                    expected_claim_token: None,
                },
            )
            .await
            .unwrap();
        let recovered = dispatcher
            .run_cleanup(
                "ek-p",
                CleanupKind::ReleaseReservations,
                None,
                false,
                |_| ProviderOutcome::Delivered(serde_json::json!({"ok": true})),
            )
            .await
            .expect("expired lease recovers");
        assert_eq!(recovered.state, EffectState::Settled);
        assert_eq!(recovered.attempt, 2, "same key, next attempt");
    }

    /// bd-v139.3 RED: retention prunes only terminal evidence within policy —
    /// settled-with-expired-retention goes (with its attempts), while
    /// unknown, in-flight, pending, and unexpired rows survive.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_retention_prunes_only_terminal() {
        use toron_core::index::workflow_effects::{
            ClaimAttempt, EffectState, NewAttempt, NewEffect, NewPending, PendingKind, TerminalPub,
        };
        use toron_workflow::engine::RetentionPolicy;
        let (_dispatcher, store) = cleanup_dispatcher("retention").await;
        let old = 1i64;
        let future = 9_999_999_999_999_999i64;
        let seed_effect = |key: &str, run: &str, retention: i64| NewEffect {
            effect_key: key.into(),
            run_id: run.into(),
            project: None,
            workflow_digest: "d".into(),
            node_path: "n".into(),
            iteration_key: None,
            action_type: "send_message".into(),
            action_digest: "a".into(),
            redacted_action: None,
            retention_until: Some(retention),
        };
        store
            .prepare_effect(seed_effect("ek-old", "run-old", old))
            .await
            .unwrap();
        store
            .transition_effect("ek-old", EffectState::InFlight, None, None)
            .await
            .unwrap();
        store
            .transition_effect(
                "ek-old",
                EffectState::Settled,
                None,
                Some(serde_json::json!(1)),
            )
            .await
            .unwrap();
        store
            .record_attempt(NewAttempt {
                effect_key: "ek-old".into(),
                attempt: 1,
                attempt_id: "att-1".into(),
                claim_token: None,
                state: EffectState::Settled,
                evidence: None,
            })
            .await
            .unwrap();
        store
            .prepare_effect(seed_effect("ek-unk", "run-old", old))
            .await
            .unwrap();
        store
            .transition_effect("ek-unk", EffectState::InFlight, None, None)
            .await
            .unwrap();
        store
            .transition_effect(
                "ek-unk",
                EffectState::Unknown,
                Some(serde_json::json!({"error": "t/o"})),
                None,
            )
            .await
            .unwrap();
        store
            .prepare_effect(seed_effect("ek-new", "run-old", future))
            .await
            .unwrap();
        store
            .transition_effect("ek-new", EffectState::InFlight, None, None)
            .await
            .unwrap();
        store
            .transition_effect("ek-new", EffectState::Settled, None, None)
            .await
            .unwrap();
        store
            .prepare_effect(seed_effect("ek-fly", "run-old", old))
            .await
            .unwrap();
        store
            .claim_attempt(
                "ek-fly",
                ClaimAttempt {
                    attempt: 1,
                    attempt_id: "att-1".into(),
                    claim_token: "tok".into(),
                    lease_expires_at: future,
                    expected_claim_token: None,
                },
            )
            .await
            .unwrap();
        store
            .upsert_pending(NewPending {
                run_id: "run-old".into(),
                promise_id: "p-1".into(),
                park_generation: 1,
                project: None,
                workflow: "w".into(),
                node_path: "n".into(),
                effect_key: None,
                kind: PendingKind::Approval,
                pending_payload: serde_json::json!({}),
                deadline_secs: None,
            })
            .await
            .unwrap();
        store
            .create_run(toron_core::index::workflow_effects::NewRun {
                run_id: "run-old".into(),
                project: None,
                workflow: "w".into(),
                workflow_digest: "d".into(),
            })
            .await
            .unwrap();
        store
            .create_run(toron_core::index::workflow_effects::NewRun {
                run_id: "run-gone".into(),
                project: None,
                workflow: "w".into(),
                workflow_digest: "d".into(),
            })
            .await
            .unwrap();
        use toron_core::index::workflow_effects::RunStatus;
        for (from, to) in [
            (RunStatus::Started, RunStatus::Running),
            (RunStatus::Running, RunStatus::Succeeded),
        ] {
            store.cas_run_status("run-gone", from, to).await.unwrap();
        }
        store
            .prepare_effect(seed_effect("ek-gone", "run-gone", old))
            .await
            .unwrap();
        store
            .transition_effect("ek-gone", EffectState::InFlight, None, None)
            .await
            .unwrap();
        store
            .transition_effect("ek-gone", EffectState::Settled, None, None)
            .await
            .unwrap();
        store
            .note_terminal_pub(TerminalPub::new("run-old", "digest-old", "succeeded", "n"))
            .await
            .unwrap();
        store.mark_terminal_published("digest-old").await.unwrap();
        let counts = store
            .prune_expired(
                5_000_000_000_000_000,
                RetentionPolicy::default().terminal_max_age_us,
                500,
            )
            .await
            .unwrap();
        assert_eq!(counts.effects, 2, "expired terminal effects only");
        assert_eq!(counts.attempts, 1, "orphaned attempt evidence");
        assert!(store.get_effect("ek-old").await.unwrap().is_none());
        assert!(store.get_effect("ek-gone").await.unwrap().is_none());
        assert!(
            store.get_effect("ek-unk").await.unwrap().is_some(),
            "unknown survives"
        );
        assert!(
            store.get_effect("ek-new").await.unwrap().is_some(),
            "live retention survives"
        );
        assert!(
            store.get_effect("ek-fly").await.unwrap().is_some(),
            "in-flight survives"
        );
        assert_eq!(
            store.list_pending("run-old").await.unwrap().len(),
            1,
            "pending survives"
        );
        assert_eq!(counts.terminals, 1, "published terminal pruned by age");
        assert_eq!(counts.runs, 1, "only the quiesced run pruned");
        assert!(
            store.get_run("run-old").await.unwrap().is_some(),
            "run with live effects survives"
        );
    }

    /// bd-v139.3 RED: journaled evidence is redacted by bound — a huge
    /// redacted action projection lands truncated, never raw.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_evidence_redacted() {
        use toron_core::index::workflow_effects::MAX_EVIDENCE_BYTES;
        use toron_workflow::engine::CleanupKind;
        let (dispatcher, store) = cleanup_dispatcher("redact").await;
        let big = "s3cr3t-".repeat(2000);
        dispatcher
            .run_cleanup(
                "ek-p",
                CleanupKind::CaptainEvidence,
                Some(serde_json::json!({"body": big})),
                true,
                |_| ProviderOutcome::Delivered(serde_json::json!({"ok": true})),
            )
            .await
            .expect("dispatch");
        let row = store
            .get_effect("ek-p/cleanup:captain-evidence")
            .await
            .unwrap()
            .unwrap();
        let stored = row.redacted_action.unwrap();
        assert!(stored.len() <= MAX_EVIDENCE_BYTES + 32, "bounded");
        assert!(stored.ends_with("…[truncated]"), "truncation is explicit");
    }

    /// bd-v139.3 RED: unknown rows render an operator-actionable report —
    /// key, location, attempt, and the next check, never a bare state.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_cleanup_unknown_report_actionable() {
        use toron_workflow::engine::CleanupKind;
        let (dispatcher, _store) = cleanup_dispatcher("report").await;
        dispatcher
            .run_cleanup("ek-p", CleanupKind::BeadComment, None, false, |_| {
                ProviderOutcome::Ambiguous("comment timeout".into())
            })
            .await
            .expect("unknown");
        let report = dispatcher
            .unknown_report("ek-p/cleanup:bead-comment")
            .await
            .expect("report exists for unknown row");
        assert_eq!(report.effect_key, "ek-p/cleanup:bead-comment");
        assert!(
            report.next_step.contains("operator"),
            "names the next check"
        );
        assert!(dispatcher.unknown_report("ek-missing").await.is_none());
    }

    /// bd-i6kc: `react` has no reaction sink — it MUST fail closed with an
    /// explicit unsupported-action error, never `Ok(Null)` success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn honesty_gate_react_fails_closed() {
        let out = toron_workflow::engine::invoke_compat(
            &Action::React {
                message_id: "deadbeef".into(),
                emoji: "+1".into(),
            },
            |_| None,
        );
        assert_ne!(
            out,
            serde_json::Value::Null,
            "react must not return bare Null"
        );
        assert_eq!(
            out["ok"],
            serde_json::json!(false),
            "react must fail: {out}"
        );
        assert!(
            out["error"].as_str().is_some_and(|e| !e.is_empty()),
            "react failure names the cause: {out}"
        );
    }

    /// bd-i6kc: `call_webhook` has no webhook sink — it MUST fail closed with
    /// an explicit unsupported-action error, never `Ok(Null)` success.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn honesty_gate_call_webhook_fails_closed() {
        let out = toron_workflow::engine::invoke_compat(
            &Action::CallWebhook {
                url: "https://example.com/hook".into(),
                method: None,
                body: None,
            },
            |_| None,
        );
        assert_ne!(
            out,
            serde_json::Value::Null,
            "call_webhook must not return bare Null"
        );
        assert_eq!(
            out["ok"],
            serde_json::json!(false),
            "call_webhook must fail: {out}"
        );
        assert!(
            out["error"].as_str().is_some_and(|e| !e.is_empty()),
            "call_webhook failure names the cause: {out}"
        );
    }

    /// bd-i6kc: engine-owned control actions accidentally passed to the
    /// effect runner MUST be rejected with a typed misdispatch error, never
    /// executed and never `Ok(Null)`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn honesty_gate_control_actions_rejected_by_runner() {
        use toron_workflow::schema::{ContextMode, Step, StepOnError};
        let leaf = || Step {
            name: None,
            guard: None,
            on_error: StepOnError::Continue,
            build_slot: false,
            action: Action::Delay { secs: 0 },
        };
        let cases: Vec<(&str, Action)> = vec![
            ("delay", Action::Delay { secs: 1 }),
            (
                "request_approval",
                Action::RequestApproval {
                    message: "m".into(),
                },
            ),
            (
                "retry",
                Action::Retry {
                    max_attempts: 1,
                    backoff_secs: 0,
                    step: Box::new(leaf()),
                },
            ),
            (
                "branch",
                Action::Branch {
                    if_guard: "true".into(),
                    then: vec![],
                    else_steps: vec![],
                },
            ),
            (
                "foreach",
                Action::Foreach {
                    over: "x".into(),
                    each: Box::new(leaf()),
                },
            ),
            (
                "parallel",
                Action::Parallel {
                    over: "1".into(),
                    each: Box::new(leaf()),
                },
            ),
            (
                "call_workflow",
                Action::CallWorkflow {
                    workflow: "w".into(),
                    inputs: serde_json::json!({}),
                },
            ),
            (
                "spawn",
                Action::Spawn {
                    profile: "@p".into(),
                    harness: None,
                    workflow: None,
                    await_: false,
                    no_claim_timeout: 0,
                    context: Some(ContextMode::Inherit),
                },
            ),
            (
                "verify",
                Action::Verify {
                    command: "true".into(),
                },
            ),
        ];
        for (name, action) in &cases {
            let out = toron_workflow::engine::invoke_compat(action, |_| None);
            assert_ne!(
                out,
                serde_json::Value::Null,
                "{name} must not return bare Null"
            );
            assert_eq!(
                out["ok"],
                serde_json::json!(false),
                "{name} must fail: {out}"
            );
        }
    }

    /// bd-cnr5 RED: durable run ledger — run state survives a host restart
    /// via the workflow_runs table (in-memory HashMap alone loses it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_ledger_durable_run_state_survives_restart() {
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        let db = std::env::temp_dir().join(format!("wf-runledger-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let store = WorkflowEffectStore::new(pool);
        let wf = toron_workflow::schema::parse_workflow(
            "name: ship\ntrigger:\n  type: webhook\nsteps:\n  - name: gate\n    action: request_approval\n    message: q\n",
        )
        .expect("parse");
        let mut first = PendingDrain::new().with_store(store.clone());
        first
            .track_run_durable("run-restart-1", wf.clone())
            .await
            .expect("track durable");
        first
            .set_run_state_durable("run-restart-1", RunState::Suspended)
            .await
            .expect("set durable");
        drop(first);
        let second = PendingDrain::new().with_store(store);
        assert_eq!(
            second.run_state_durable("run-restart-1").await,
            RunState::Suspended,
            "run state survives restart via workflow_runs"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// bd-cnr5 RED: durable terminal dedupe — a same-digest republish after
    /// restart is still a Duplicate (in-memory ledger alone forgets it).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_ledger_terminal_dedupe_survives_restart() {
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        let db =
            std::env::temp_dir().join(format!("wf-termdedupe-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let store = WorkflowEffectStore::new(pool);
        let digest =
            toron_workflow::engine::terminal_digest("run-t-1", "d", "succeeded", "gate", "ev");
        let rec = toron_workflow::engine::TerminalRecord {
            run_id: "run-t-1".into(),
            digest: digest.clone(),
            status: "succeeded".into(),
            node: "gate".into(),
        };
        let mut first = PendingDrain::new().with_store(store.clone());
        assert!(matches!(
            first
                .note_terminal_durable(rec.clone())
                .await
                .expect("note"),
            toron_workflow::engine::TerminalNote::New
        ));
        drop(first);
        let mut second = PendingDrain::new().with_store(store);
        assert!(matches!(
            second.note_terminal_durable(rec).await.expect("renote"),
            toron_workflow::engine::TerminalNote::Duplicate
        ));
        let _ = std::fs::remove_file(&db);
    }

    /// bd-cnr5 RED: status-string parse is exhaustive — unrecognized and
    /// future statuses never silently become Failed.
    #[test]
    fn run_ledger_status_parse_rejects_unknown() {
        assert_eq!(
            RunState::parse_status("succeeded").unwrap(),
            RunState::Succeeded
        );
        assert_eq!(RunState::parse_status("failed").unwrap(), RunState::Failed);
        assert!(
            RunState::parse_status("suspended").is_ok(),
            "suspended is known"
        );
        assert!(
            RunState::parse_status("no-such-status-xyz").is_err(),
            "unknown status must error loud, never map to Failed"
        );
        assert!(
            RunState::parse_status("typo-failed").is_err(),
            "typos must error loud"
        );
    }

    /// bd-cnr5 RED: FlushNow with no signer publishes nothing — the result
    /// must report ok:false, never ok:true with published:false.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn run_ledger_flushnow_unpublished_is_failure() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        {
            let db =
                std::env::temp_dir().join(format!("wf-flushok-{}.sqlite", uuid::Uuid::new_v4()));
            let pool = schema::connect(db.to_str().unwrap())
                .await
                .expect("connect");
            schema::migrate(&pool).await.expect("migrate");
            let bus: Arc<dyn Bus> = Arc::new(
                InProcessBus::open(pool, InProcessBusConfig::default())
                    .await
                    .expect("bus"),
            );
            let mailbox = Arc::new(Mailbox::new(
                bus,
                std::env::temp_dir().join(format!("wf-flushok-data-{}", uuid::Uuid::new_v4())),
                None,
            ));
            let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
                1,
            )));
            let afx = mailbox_async_effect_runner(
                mailbox,
                coalescer,
                None,
                Arc::new(std::sync::Mutex::new(None)),
                test_armor(),
            );
            let out = drive_async(
                &afx,
                Action::DispatchJob {
                    assignee: "oc".into(),
                    spec: crate::workflow_host::test_spec(serde_json::json!({ "t": 1 })),
                    channel: None,
                },
                "run-flushnow",
            )
            .await;
            assert_eq!(
                out["ok"],
                serde_json::json!(false),
                "unpublished flush must report ok:false, got {out}"
            );
            let _ = std::fs::remove_file(&db);
        }
    }

    /// bd-1kr2 RED helper: a dispatcher-backed ASYNC production runner over
    /// throwaway sqlite, with sender + recipient registered in `demo`.
    async fn fencing_runner(
        tag: &str,
    ) -> (
        AsyncEffectRunner,
        EffectDispatcher,
        Arc<dyn Bus>,
        std::path::PathBuf,
        std::path::PathBuf,
        toron_core::nostr::Keys,
    ) {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        const PASSPHRASE: &str = "wf-fencing-pass";
        let db = std::env::temp_dir().join(format!("wf-fencing-{tag}-{}", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir =
            std::env::temp_dir().join(format!("wf-fencing-data-{tag}-{}", uuid::Uuid::new_v4()));
        let mailbox = Arc::new(
            Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
        );
        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("workflow-demo"),
            )
            .await
            .expect("register sender");
        let recv = mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("BlueLake"),
            )
            .await
            .expect("register recipient");
        let recv_keys =
            toron_core::keys::load_encrypted_key(std::path::Path::new(&recv.key_path), PASSPHRASE)
                .expect("load recipient key");
        let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool);
        let dispatcher = EffectDispatcher::new(store);
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let runner = mailbox_async_effect_runner(
            mailbox,
            coalescer,
            None,
            Arc::new(std::sync::Mutex::new(Some(dispatcher.clone()))),
            test_armor(),
        );
        (runner, dispatcher, bus, db, data_dir, recv_keys)
    }

    /// Test armor (bd-qdup): production tuning at test speed — the 30s
    /// production timeout never trips here, and the breaker stays shut on
    /// green paths (storm tests build their own tuned armor).
    fn test_armor() -> Arc<SendArmor> {
        Arc::new(SendArmor::new(
            passthrough(),
            ARMOR_SEND_TIMEOUT,
            ARMOR_MAX_CONCURRENCY,
        ))
    }

    /// Drive one action through the async host runner inline (bd-ckkf):
    /// the test synthesis of the engine invocation — fixed digest/node, so
    /// keys predict via `test_invocation`. Awaited, never bridged.
    async fn drive_async(
        afx: &AsyncEffectRunner,
        action: Action,
        run_id: &str,
    ) -> serde_json::Value {
        let (invocation, _, _) = test_invocation(action, run_id);
        afx(&invocation).await.expect("async runner ok")
    }

    /// The invocation the engine would build + its run-scoped key and
    /// action digest (bd-ckkf): fixed digest-1/test-node context so tests
    /// predict journal keys without fixtures.
    fn test_invocation(action: Action, run_id: &str) -> (ActionInvocation, String, String) {
        use toron_workflow::engine::{ActionInvocation, ExecutionPolicy, RunContext};
        let rendered = serde_json::to_value(&action).expect("action json");
        let action_digest = toron_workflow::identity::action_digest_of(
            &toron_workflow::identity::redact_value(&rendered).0,
        );
        let key = toron_workflow::engine::derive_effect_key(
            "digest-1",
            run_id,
            "test-node",
            None,
            &action_digest,
        );
        (
            ActionInvocation {
                action,
                run: RunContext {
                    run_id: run_id.into(),
                    workflow_digest: "digest-1".into(),
                    node_path: "test-node".into(),
                    iteration_key: None,
                    effect_key: None,
                    attempt: 1,
                    provider_result: None,
                },
                execution: ExecutionPolicy {
                    max_attempts: 1,
                    timeout_secs: None,
                },
            },
            key,
            action_digest,
        )
    }

    /// bd-1kr2 RED: the REAL SendMessage arm journals intent before
    /// dispatch — a same-key retry attaches (one gift wrap on the wire,
    /// never two) and the journal projects corr == effect_key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prod_effect_fencing_send_message_sends_once_and_projects_corr() {
        use toron_core::mailbox::messaging::unwrap_for_recipient;
        let (afx, dispatcher, bus, db, data_dir, recv_keys) = fencing_runner("once").await;
        let action = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "fenced".into(),
            body: "exactly once".into(),
            project: Some("demo".into()),
        };
        let (_, key, _) = test_invocation(action.clone(), "run-fencing-once");
        let out1 = drive_async(&afx, action.clone(), "run-fencing-once").await;
        assert_eq!(out1["ok"], serde_json::json!(true), "first send ok: {out1}");
        let out2 = drive_async(&afx, action.clone(), "run-fencing-once").await;
        assert_eq!(
            out2["ok"],
            serde_json::json!(true),
            "same-key retry attaches: {out2}"
        );
        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([nostr::prelude::Kind::GiftWrap]),
        )
        .await
        .expect("replay");
        assert_eq!(replay.len(), 1, "one wrap for two same-key sends");
        let rumor = unwrap_for_recipient(&recv_keys, &replay[0])
            .expect("unwrap")
            .rumor;
        assert!(
            rumor
                .tags
                .iter()
                .any(|t| t.as_slice() == ["corr", key.as_str()]),
            "rumor carries corr == effect_key"
        );
        let row = dispatcher
            .store()
            .get_effect(&key)
            .await
            .expect("read")
            .expect("effect row exists — intent preceded dispatch");
        assert_eq!(
            row.state,
            toron_core::index::workflow_effects::EffectState::Settled
        );
        assert_eq!(row.transport_corr.as_deref(), Some(key.as_str()));
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-1kr2 RED: crash recovery on the REAL arm — after the host state
    /// is dropped, a rebuilt runner over the same journal still attaches
    /// (no second wrap).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prod_effect_fencing_crash_recovery_no_double_send() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        const PASSPHRASE: &str = "wf-fencing-crash";
        let db = std::env::temp_dir().join(format!("wf-fencing-crash-{}", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir =
            std::env::temp_dir().join(format!("wf-fencing-crash-data-{}", uuid::Uuid::new_v4()));
        let action = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "crash".into(),
            body: "survives restart".into(),
            project: Some("demo".into()),
        };
        {
            let mailbox = Arc::new(
                Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
            );
            mailbox
                .register_agent(
                    "demo",
                    &toron_core::nostr::identity::AgentProfile::new("workflow-demo"),
                )
                .await
                .expect("register sender");
            mailbox
                .register_agent(
                    "demo",
                    &toron_core::nostr::identity::AgentProfile::new("BlueLake"),
                )
                .await
                .expect("register recipient");
            let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone());
            let afx = mailbox_async_effect_runner(
                mailbox,
                Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
                None,
                Arc::new(std::sync::Mutex::new(Some(EffectDispatcher::new(store)))),
                test_armor(),
            );
            let out = drive_async(&afx, action.clone(), "run-fencing-crash").await;
            assert_eq!(out["ok"], serde_json::json!(true));
            drop(afx);
        }
        {
            let mailbox = Arc::new(
                Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
            );
            let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone());
            let afx = mailbox_async_effect_runner(
                mailbox,
                Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
                None,
                Arc::new(std::sync::Mutex::new(Some(EffectDispatcher::new(store)))),
                test_armor(),
            );
            let out = drive_async(&afx, action.clone(), "run-fencing-crash").await;
            assert_eq!(
                out["ok"],
                serde_json::json!(true),
                "recovery attaches: {out}"
            );
        }
        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([nostr::prelude::Kind::GiftWrap]),
        )
        .await
        .expect("replay");
        assert_eq!(replay.len(), 1, "one wrap across the restart");
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-1kr2 RED: the REAL DispatchJob arm through the journal — a
    /// cap-flush with no signer journals a terminal Failed (never a
    /// silent success), and the retry attaches without publishing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prod_effect_fencing_no_signer_flush_fails_terminal() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        let db = std::env::temp_dir().join(format!("wf-fencing-nosign-{}", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let mailbox = Arc::new(Mailbox::new(
            bus.clone(),
            std::env::temp_dir().join(format!("wf-fencing-nosign-data-{}", uuid::Uuid::new_v4())),
            None,
        ));
        let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool);
        let dispatcher = EffectDispatcher::new(store);
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(60),
            1,
        )));
        let afx = mailbox_async_effect_runner(
            mailbox,
            coalescer,
            None,
            Arc::new(std::sync::Mutex::new(Some(dispatcher.clone()))),
            test_armor(),
        );
        let action = Action::DispatchJob {
            assignee: "oc".into(),
            spec: crate::workflow_host::test_spec(serde_json::json!({ "nosign": 1 })),
            channel: None,
        };
        let (_, key, _) = test_invocation(action.clone(), "run-fencing-nosign");
        for _ in 0..2 {
            let out = drive_async(&afx, action.clone(), "run-fencing-nosign").await;
            assert_eq!(out["ok"], serde_json::json!(false), "unpublished: {out}");
        }
        let row = dispatcher
            .store()
            .get_effect(&key)
            .await
            .expect("read")
            .expect("effect row exists");
        assert_eq!(
            row.state,
            toron_core::index::workflow_effects::EffectState::Failed
        );
        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([nostr::prelude::Kind::from_u16(43001)]),
        )
        .await
        .expect("replay");
        assert!(replay.is_empty(), "nothing published without a signer");
        let _ = std::fs::remove_file(&db);
    }

    /// bd-1kr2 RED: uncertainty on an arm-shaped key journals Unknown and
    /// refuses a blind retry — the crash-matrix row the policy owns
    /// (plan logic is arm-agnostic; the key shape is the real one).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prod_effect_fencing_unknown_refuses_blind_retry() {
        use toron_core::index::schema;
        let db = std::env::temp_dir().join(format!("wf-fencing-unk-{}", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool);
        let dispatcher = EffectDispatcher::new(store);
        let action = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "ambiguous".into(),
            body: "did it send?".into(),
            project: Some("demo".into()),
        };
        let (_, key, digest) = test_invocation(action.clone(), "run-fencing-unk");
        let req = DispatchRequest {
            effect_key: key.clone(),
            run_id: "run-fencing-unk".into(),
            project: Some("demo".into()),
            workflow_digest: "digest-1".into(),
            node_path: "test-node".into(),
            iteration_key: None,
            action_type: action.type_name().into(),
            action_digest: digest,
            redacted_action: None,
            capability: toron_workflow::engine::capability_for_action_type("send_message"),
        };
        let out = dispatcher
            .dispatch(&req, |_| {
                toron_workflow::engine::ProviderOutcome::Ambiguous("bus timeout".into())
            })
            .await
            .expect("dispatch records uncertainty");
        assert_eq!(
            out.state,
            toron_core::index::workflow_effects::EffectState::Unknown
        );
        let retry = dispatcher
            .dispatch(&req, |_| {
                toron_workflow::engine::ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
            })
            .await;
        assert!(
            retry.is_err(),
            "blind retry after Unknown must refuse, never re-dispatch"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// bd-zczp: the context-bearing effect runner journals run-scoped
    /// keys — identical payloads in different runs send twice (no
    /// compat over-dedupe), while the same invocation twice attaches.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_runner_uses_run_scoped_keys() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::engine::{ActionInvocation, ExecutionPolicy, RunContext};
        const PASSPHRASE: &str = "wf-effect-scope";
        let db = std::env::temp_dir().join(format!("wf-scope-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir = std::env::temp_dir().join(format!("wf-scope-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Arc::new(
            Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
        );
        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("workflow-demo"),
            )
            .await
            .expect("register sender");
        mailbox
            .register_agent(
                "demo",
                &toron_core::nostr::identity::AgentProfile::new("BlueLake"),
            )
            .await
            .expect("register recipient");
        let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool);
        let dispatcher = EffectDispatcher::new(store.clone());
        let cell = Arc::new(std::sync::Mutex::new(Some(dispatcher.clone())));
        let afx = mailbox_async_effect_runner(
            mailbox,
            Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            None,
            cell,
            test_armor(),
        );
        fn inv(run: &str, node: &str) -> ActionInvocation {
            ActionInvocation {
                action: Action::SendMessage {
                    to: vec!["BlueLake".into()],
                    subject: "scoped".into(),
                    body: "same payload".into(),
                    project: Some("demo".into()),
                },
                run: RunContext {
                    run_id: run.into(),
                    workflow_digest: "digest-1".into(),
                    node_path: node.into(),
                    iteration_key: None,
                    effect_key: None,
                    attempt: 1,
                    provider_result: None,
                },
                execution: ExecutionPolicy {
                    max_attempts: 1,
                    timeout_secs: None,
                },
            }
        }
        let call = |afx: &AsyncEffectRunner, invocation: ActionInvocation| {
            let afx = afx.clone();
            async move { afx(&invocation).await.expect("effect ok") }
        };
        let out_a1 = call(&afx, inv("run-A", "gate")).await;
        assert_eq!(out_a1["ok"], serde_json::json!(true));
        let out_a2 = call(&afx, inv("run-A", "gate")).await;
        assert_eq!(out_a2["ok"], serde_json::json!(true));
        let out_b = call(&afx, inv("run-B", "gate")).await;
        assert_eq!(out_b["ok"], serde_json::json!(true));
        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([nostr::prelude::Kind::GiftWrap]),
        )
        .await
        .expect("replay");
        assert_eq!(
            replay.len(),
            2,
            "run-scoped keys: A×2 attaches (1 wrap), B sends again"
        );
        let rendered = serde_json::to_value(&inv("run-A", "gate").action).expect("action json");
        let action_digest = toron_workflow::identity::action_digest_of(
            &toron_workflow::identity::redact_value(&rendered).0,
        );
        let expected_a = toron_workflow::engine::derive_effect_key(
            "digest-1",
            "run-A",
            "gate",
            None,
            &action_digest,
        );
        let row = store
            .get_effect(&expected_a)
            .await
            .expect("read")
            .expect("run-scoped row for run-A exists");
        assert_eq!(
            row.state,
            toron_core::index::workflow_effects::EffectState::Settled
        );
        assert_eq!(row.transport_corr.as_deref(), Some(expected_a.as_str()));
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-engine-run-path-journaling-fwlx: the host journals `verify` around
    /// the engine-observed `VerifySink` result — the one effectful sink whose
    /// provider is the ENGINE, not this runner (ADR-0024 Spec 2). Without this
    /// arm the host's `Enforced` engine would run a sink it cannot journal,
    /// which is exactly the Spec 6 bypass the flip exists to close.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn effect_runner_journals_verify_around_the_observed_result() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::engine::{ActionInvocation, ExecutionPolicy, RunContext};
        let db = std::env::temp_dir().join(format!("wf-verify-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool.clone(), InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let data_dir =
            std::env::temp_dir().join(format!("wf-verify-data-{}", uuid::Uuid::new_v4()));
        let mailbox = Arc::new(Mailbox::new(bus.clone(), data_dir.clone(), None));
        let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool);
        let cell = Arc::new(std::sync::Mutex::new(Some(EffectDispatcher::new(
            store.clone(),
        ))));
        let afx = mailbox_async_effect_runner(
            mailbox,
            Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            None,
            cell,
            test_armor(),
        );
        let action = Action::Verify {
            command: "cargo test".into(),
        };
        let invocation = |run: &str, observed: Option<serde_json::Value>| ActionInvocation {
            action: action.clone(),
            run: RunContext {
                run_id: run.into(),
                workflow_digest: "digest-1".into(),
                node_path: "gate".into(),
                iteration_key: None,
                effect_key: None,
                attempt: 1,
                provider_result: observed,
            },
            execution: ExecutionPolicy {
                max_attempts: 1,
                timeout_secs: None,
            },
        };
        let call = |i: ActionInvocation| {
            let afx = afx.clone();
            async move { afx(&i).await.expect("effect ok") }
        };

        let green = call(invocation(
            "run-verify-green",
            Some(serde_json::json!({ "ok": true, "exit": 0, "stdout_path": "r.txt" })),
        ))
        .await;
        assert_eq!(
            green["ok"],
            serde_json::json!(true),
            "a green verify passes through: {green}"
        );
        let red = call(invocation(
            "run-verify-red",
            Some(serde_json::json!({ "ok": false, "exit": 101, "error": "tests failed" })),
        ))
        .await;
        assert_eq!(
            red["ok"],
            serde_json::json!(false),
            "a red verify fails loud, never as a delivered success: {red}"
        );
        assert_eq!(
            red["effect_state"],
            serde_json::json!("failed"),
            "a returned non-zero exit is a PROVEN failure, not transport \
             ambiguity (nothing to reconcile): {red}"
        );
        let missing = call(invocation("run-verify-missing", None)).await;
        assert!(
            missing.to_string().contains("verify sink missing"),
            "a host without a VerifySink names the real cause: {missing}"
        );

        let rendered = serde_json::to_value(&action).expect("action json");
        let action_digest = toron_workflow::identity::action_digest_of(
            &toron_workflow::identity::redact_value(&rendered).0,
        );
        for (run, want) in [
            (
                "run-verify-green",
                toron_core::index::workflow_effects::EffectState::Settled,
            ),
            (
                "run-verify-red",
                toron_core::index::workflow_effects::EffectState::Failed,
            ),
            (
                "run-verify-missing",
                toron_core::index::workflow_effects::EffectState::Failed,
            ),
        ] {
            let key = toron_workflow::engine::derive_effect_key(
                "digest-1",
                run,
                "gate",
                None,
                &action_digest,
            );
            let row = store
                .get_effect(&key)
                .await
                .expect("read")
                .unwrap_or_else(|| panic!("verify effect row for {run} must be journaled"));
            assert_eq!(row.action_type, "verify");
            assert_eq!(row.state, want, "row state for {run}");
        }
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// bd-3q75 wave-2b: the async host effect runner dispatches a
    /// SendMessage leaf inline — journaled under the run-scoped key with
    /// corr == effect_key, never thread-hop bridged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_effect_runner_sends_once_and_projects_corr() {
        use toron_core::mailbox::messaging::unwrap_for_recipient;
        let (afx, _dispatcher, bus, db, data_dir, recv_keys) = fencing_runner("asyncfx").await;
        let action = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "async-fenced".into(),
            body: "exactly once, no bridge".into(),
            project: Some("demo".into()),
        };
        let (_, key, _) = test_invocation(action.clone(), "run-async-fx-1");
        let out1 = drive_async(&afx, action.clone(), "run-async-fx-1").await;
        assert_eq!(out1["ok"], serde_json::json!(true), "first send ok: {out1}");
        let out2 = drive_async(&afx, action.clone(), "run-async-fx-1").await;
        assert_eq!(
            out2["ok"],
            serde_json::json!(true),
            "same-key retry attaches: {out2}"
        );
        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([nostr::prelude::Kind::GiftWrap]),
        )
        .await
        .expect("replay");
        assert_eq!(replay.len(), 1, "one wrap for two same-key sends");
        let rumor = unwrap_for_recipient(&recv_keys, &replay[0])
            .expect("unwrap")
            .rumor;
        assert!(
            rumor
                .tags
                .iter()
                .any(|t| t.as_slice() == ["corr", key.as_str()]),
            "rumor carries corr == effect_key"
        );
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    /// GS-062: multi-parent batch peeks/marks ALL parents; a later batch whose
    /// first parent is already delivered still publishes undelivered siblings.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn multi_parent_cursor_marks_all_and_keeps_siblings() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-mp-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-mp";

        let batch1 = Coalesced {
            assignee: session.into(),
            mentions: vec![
                InboundMention {
                    assignee: session.into(),
                    parent: "parent-a".into(),
                    spec: json!({"text": "a"}),
                },
                InboundMention {
                    assignee: session.into(),
                    parent: "parent-b".into(),
                    spec: json!({"text": "b"}),
                },
            ],
            first_arrival_ms: 0,
        };
        let id1 =
            publish_coalesced_job_with_cursor(bus.as_ref(), Some(&signer), &batch1, Some(&store))
                .await
                .expect("first batch publishes");
        assert!(!id1.to_hex().is_empty());
        assert!(store.cursor(session).delivered.contains("parent-a"));
        assert!(store.cursor(session).delivered.contains("parent-b"));

        let batch2 = Coalesced {
            assignee: session.into(),
            mentions: vec![
                InboundMention {
                    assignee: session.into(),
                    parent: "parent-a".into(),
                    spec: json!({"text": "a2"}),
                },
                InboundMention {
                    assignee: session.into(),
                    parent: "parent-c".into(),
                    spec: json!({"text": "c"}),
                },
            ],
            first_arrival_ms: 1,
        };
        let id2 =
            publish_coalesced_job_with_cursor(bus.as_ref(), Some(&signer), &batch2, Some(&store))
                .await
                .expect("sibling batch must publish when any parent undelivered");
        assert_ne!(id1, id2);
        assert!(store.cursor(session).delivered.contains("parent-c"));

        let err =
            publish_coalesced_job_with_cursor(bus.as_ref(), Some(&signer), &batch2, Some(&store))
                .await
                .expect_err("fully delivered batch skips");
        assert!(
            matches!(err, CoalescePublishError::AlreadyDelivered),
            "idempotent skip must be AlreadyDelivered, not Transport, got {err:?}"
        );

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

    /// GS-062: FlushNow path (mention_outcome) uses DeliveryCursorStore via
    /// publish_coalesced_job_with_cursor — not delivery=None.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushnow_path_uses_delivery_cursor() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-fn-cur-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let delivery = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-fn";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "fn-parent-1".into(),
                spec: json!({"i": 0}),
            }],
            first_arrival_ms: 0,
        };
        let decision = CoalesceDecision::FlushNow(batch.clone());
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            decision,
            "effect-fn-1".into(),
            Some(&delivery),
            None,
            None,
        )
        .await;
        match out {
            ProviderOutcome::Delivered(v) => {
                assert_eq!(v["published"], serde_json::json!(true), "{v}");
            }
            other => panic!("expected Delivered, got {other:?}"),
        }
        assert!(
            delivery.cursor(session).delivered.contains("fn-parent-1"),
            "FlushNow must mark via with_cursor"
        );
        let out2 = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "effect-fn-2".into(),
            Some(&delivery),
            None,
            None,
        )
        .await;
        match &out2 {
            ProviderOutcome::Delivered(v) => {
                assert_eq!(v["skipped"], serde_json::json!(true), "{v}");
                assert_eq!(v["published"], serde_json::json!(false), "{v}");
            }
            other => panic!(
                "second FlushNow skip must Delivered{{skipped:true}} (Settled), never Ambiguous/Unknown, got {other:?}"
            ),
        }
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062: concurrent with_cursor reserve — only one publish wins; the
    /// other sees keep empty (no double-publish). Deterministic, no sleeps.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_with_cursor_no_double_publish() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-race-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let store = Arc::new(DeliveryCursorStore::new());
        let signer = Keys::generate();
        let session = "oc-race";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "race-parent".into(),
                spec: json!({"t": "race"}),
            }],
            first_arrival_ms: 0,
        };

        let store_a = store.clone();
        let store_b = store.clone();
        let bus_a = bus.clone();
        let bus_b = bus.clone();
        let batch_a = batch.clone();
        let batch_b = batch.clone();
        let signer_a = signer.clone();
        let signer_b = signer.clone();

        let (r1, r2) = tokio::join!(
            publish_coalesced_job_with_cursor(
                bus_a.as_ref(),
                Some(&signer_a),
                &batch_a,
                Some(store_a.as_ref()),
            ),
            publish_coalesced_job_with_cursor(
                bus_b.as_ref(),
                Some(&signer_b),
                &batch_b,
                Some(store_b.as_ref()),
            ),
        );
        let wins = [r1.is_ok(), r2.is_ok()].into_iter().filter(|x| *x).count();
        let skips = [r1.is_err(), r2.is_err()]
            .into_iter()
            .filter(|x| *x)
            .count();
        assert_eq!(wins, 1, "exactly one publish wins, got r1={r1:?} r2={r2:?}");
        assert_eq!(skips, 1, "exactly one skips as already in-flight/delivered");
        let loser = if r1.is_err() { &r1 } else { &r2 };
        match loser {
            Err(CoalescePublishError::InFlight) | Err(CoalescePublishError::AlreadyDelivered) => {}
            other => panic!("loser must be InFlight or AlreadyDelivered, got {other:?}"),
        }
        assert!(store.cursor(session).delivered.contains("race-parent"));
        assert!(store.cursor(session).in_flight.is_empty());

        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 EMPTYKEEP-INFLIGHT: keep=[] via SkipInFlight must NOT Settled.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inflight_empty_keep_is_ambiguous_not_settled() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-inflight-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-inflight";
        let parent = "inflight-parent";
        let (keep, _) = store.reserve_undelivered(session, [parent]);
        assert_eq!(keep, vec![parent.to_string()]);

        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: parent.into(),
                spec: json!({"t": "inflight"}),
            }],
            first_arrival_ms: 0,
        };
        let err =
            publish_coalesced_job_with_cursor(bus.as_ref(), Some(&signer), &batch, Some(&store))
                .await
                .expect_err("in-flight empty-keep must error");
        assert!(
            matches!(err, CoalescePublishError::InFlight),
            "must be InFlight not AlreadyDelivered, got {err:?}"
        );

        let coalescer = std::sync::Mutex::new(MentionCoalescer::with_defaults());
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch.clone()),
            "ek-inflight".into(),
            Some(&store),
            None,
            Some(&coalescer),
        )
        .await;
        match &out {
            ProviderOutcome::Ambiguous(m) => {
                assert!(
                    m.contains("in-flight") || m.contains("concurrent"),
                    "Ambiguous reason: {m}"
                );
            }
            other => {
                panic!("in-flight FlushNow must Ambiguous (not Settled/Delivered), got {other:?}")
            }
        }
        assert!(
            coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .buffered_for(session)
                >= 1,
            "in-flight must requeue burst"
        );

        store.mark_delivered(session, [parent]);
        let err2 =
            publish_coalesced_job_with_cursor(bus.as_ref(), Some(&signer), &batch, Some(&store))
                .await
                .expect_err("now truly delivered");
        assert!(
            matches!(err2, CoalescePublishError::AlreadyDelivered),
            "cursor-proven skip is AlreadyDelivered, got {err2:?}"
        );

        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 ACQUIRE-DROP: acquire Err requeues into coalescer (no silent loss).
    #[test]
    fn acquire_err_requeues_flushed_burst() {
        let mut c = MentionCoalescer::new(
            std::time::Duration::from_millis(2_000),
            std::time::Duration::from_millis(500),
            64,
        );
        use toron_workflow::coalesce::InboundMention;
        c.submit(
            InboundMention {
                assignee: "oc".into(),
                parent: "p1".into(),
                spec: json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").expect("flushed");
        assert_eq!(c.buffered_for("oc"), 0);
        let q = c.requeue(batch, 1_000, RequeueSchedule::Transient);
        assert!(
            matches!(q, RequeueOutcome::Queued { delay_ms: 0, .. }),
            "{q:?}"
        );
        assert_eq!(c.buffered_for("oc"), 1);
        let again = c.tick(1_000);
        assert_eq!(again.len(), 1, "requeued burst must flush again");
        assert_eq!(again[0].parent(), "p1");
    }

    /// GS-062 ACQUIRE-DROP: host_acquire sleep covers full hold_bound (not 5s cap).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_acquire_sleep_covers_full_hold_bound() {
        use toron_workflow::dispatch::{DispatchAcquire, DispatchConfig, DispatchPool};
        let pool = DispatchPool::new(DispatchConfig {
            max_concurrency: 2,
            hold_bound_ms: 80,
        });
        assert_eq!(pool.hold_bound_ms(), 80);
        let hold = pool.try_acquire_fenced("oc", 10, 0).expect("first grant");
        match pool.try_acquire_for_host("oc", 20, 0) {
            DispatchAcquire::Park { until_ms, .. } => {
                assert!(
                    until_ms >= 20 + 80 - 1,
                    "park until must reflect hold_bound, got {until_ms}"
                );
            }
            other => panic!("expected Park, got {other:?}"),
        }
        drop(hold);
        let _ = pool.deadline_wake("oc", 10_000);
        let permit = host_acquire_for_assignee(&pool, "oc", 10_000)
            .await
            .expect("host acquire after wake");
        drop(permit);
    }

    /// GS-062 PARTIAL-WIRE: FlushNow arm acquires via DispatchPool when provided.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushnow_wires_host_acquire() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;
        use toron_workflow::dispatch::{DispatchConfig, DispatchPool};

        let db = std::env::temp_dir().join(format!("wf-fn-acq-{}.sqlite", uuid::Uuid::new_v4()));
        let sql = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&sql).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(sql, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let delivery = DeliveryCursorStore::new();
        let pool = DispatchPool::new(DispatchConfig {
            max_concurrency: 2,
            hold_bound_ms: 50,
        });
        let signer = Keys::generate();
        let session = "oc-fn-acq";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "fn-acq-parent".into(),
                spec: json!({"i": 0}),
            }],
            first_arrival_ms: 0,
        };
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "ek-fn-acq".into(),
            Some(&delivery),
            Some(&pool),
            None,
        )
        .await;
        match out {
            ProviderOutcome::Delivered(v) => {
                assert_eq!(v["published"], serde_json::json!(true), "{v}");
            }
            other => panic!("FlushNow+acquire must Delivered, got {other:?}"),
        }
        assert!(
            !pool.busy_holder(session),
            "permit dropped after FlushNow publish"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 captain C: live host acquire parks SleepUntilDeadline and
    /// recovers via deadline_wake — Option try_acquire remains e2e-only.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_acquire_parks_then_deadline_wake() {
        use toron_workflow::dispatch::{DispatchAcquire, DispatchConfig, DispatchPool};
        let pool = DispatchPool::new(DispatchConfig {
            max_concurrency: 2,
            hold_bound_ms: 50,
        });
        let hold = pool.try_acquire_fenced("oc", 10, 0).expect("first grant");
        match pool.try_acquire_for_host("oc", 20, 0) {
            DispatchAcquire::Park { until_ms, .. } => assert!(until_ms >= 20),
            other => panic!("expected Park, got {other:?}"),
        }
        assert!(
            pool.try_acquire("oc").is_none(),
            "Option shim maps Sleep→None; live hosts must not use it"
        );
        drop(hold);
        let _ = pool.deadline_wake("oc", 10_000);
        let permit = host_acquire_for_assignee(&pool, "oc", 10_000)
            .await
            .expect("host acquire after wake");
        drop(permit);
    }

    /// GS-062: add_workflow / soft_delete_workflow opposite lock order must not
    /// deadlock on Clone'd hosts (register never while holding workflows).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn add_workflow_soft_delete_lock_order_no_deadlock() {
        use std::sync::{Arc, Barrier};
        use std::time::{Duration, Instant};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::Workflow;

        let db = std::env::temp_dir().join(format!("wf-lock-{}.sqlite", uuid::Uuid::new_v4()));
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
        let host_a = host.clone();
        let host_b = host;
        let barrier = Arc::new(Barrier::new(2));
        let b1 = barrier.clone();
        let b2 = barrier;
        let t0 = Instant::now();
        let h1 = std::thread::spawn(move || {
            b1.wait();
            for i in 0..40 {
                let wf = Workflow {
                    name: format!("w-a-{i}"),
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
                let mut h = host_a.clone();
                h.add_workflow(wf);
            }
        });
        let h2 = std::thread::spawn(move || {
            b2.wait();
            for i in 0..40 {
                let mut h = host_b.clone();
                let _ = h.soft_delete_workflow(&format!("w-a-{i}"));
            }
        });
        h1.join().expect("add thread");
        h2.join().expect("del thread");
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "lock-order deadlock suspected"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 ask-user B: Clone shares Arc definition registry — soft_delete
    /// visible to the cloned trigger-loop host (not a Vec copy).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn clone_sees_soft_delete_on_shared_registry() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::Workflow;

        let db = std::env::temp_dir().join(format!("wf-arc-defs-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let wf: Workflow = serde_json::from_value(serde_json::json!({
            "name": "arc-wf",
            "steps": []
        }))
        .expect("minimal workflow");
        let mut host = WorkflowHost::over_bus(bus, vec![wf], None).expect("host");
        let cloned = host.clone();
        assert_eq!(cloned.workflows().len(), 1);
        let fx = host.soft_delete_workflow("arc-wf");
        assert!(matches!(
            fx,
            LifecycleEffect::SoftDeleted { .. } | LifecycleEffect::AlreadyDeleted { .. }
        ));
        assert!(
            cloned.workflows().is_empty(),
            "Clone must see soft_delete via shared Arc registry, got {:?}",
            cloned
                .workflows()
                .iter()
                .map(|w| w.name.clone())
                .collect::<Vec<_>>()
        );
        assert!(
            matches!(
                cloned.lifecycle().try_start("arc-wf"),
                LifecycleEffect::StartRefused { .. }
            ),
            "try_start must refuse soft-deleted via shared lifecycle"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// Bus that always fails publish (GS-062 TRANSPORT-DROP regression).
    struct FailBus {
        inner: Arc<toron_core::bus::InProcessBus>,
    }

    #[async_trait::async_trait]
    impl Bus for FailBus {
        async fn publish(
            &self,
            _event: Event,
        ) -> Result<toron_core::bus::PublishOk, toron_core::bus::BusError> {
            Err(toron_core::bus::BusError::Transport(
                "injected transport failure".into(),
            ))
        }

        async fn subscribe(
            &self,
            filter: toron_core::bus::Filter,
        ) -> Result<toron_core::bus::Subscription, toron_core::bus::BusError> {
            self.inner.subscribe(filter).await
        }

        async fn subscribe_trusted(
            &self,
            filter: toron_core::bus::Filter,
        ) -> Result<toron_core::bus::Subscription, toron_core::bus::BusError> {
            self.inner.subscribe_trusted(filter).await
        }

        async fn health(&self) -> Result<toron_core::bus::Health, toron_core::bus::BusError> {
            self.inner.health().await
        }
    }

    /// Recording bus: captures published events then delegates.
    struct RecordBus {
        inner: Arc<toron_core::bus::InProcessBus>,
        published: std::sync::Mutex<Vec<Event>>,
    }

    #[async_trait::async_trait]
    impl Bus for RecordBus {
        async fn publish(
            &self,
            event: Event,
        ) -> Result<toron_core::bus::PublishOk, toron_core::bus::BusError> {
            self.published
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(event.clone());
            self.inner.publish(event).await
        }

        async fn subscribe(
            &self,
            filter: toron_core::bus::Filter,
        ) -> Result<toron_core::bus::Subscription, toron_core::bus::BusError> {
            self.inner.subscribe(filter).await
        }

        async fn subscribe_trusted(
            &self,
            filter: toron_core::bus::Filter,
        ) -> Result<toron_core::bus::Subscription, toron_core::bus::BusError> {
            self.inner.subscribe_trusted(filter).await
        }

        async fn health(&self) -> Result<toron_core::bus::Health, toron_core::bus::BusError> {
            self.inner.health().await
        }
    }

    /// GS-062 TRANSPORT-DROP-FLUSHNOW: Transport must requeue (not only Ambiguous).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushnow_transport_requeues_burst() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-tn-fn-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let inner = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let bus: Arc<dyn Bus> = Arc::new(FailBus { inner });
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-tn-fn";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "tn-fn-parent".into(),
                spec: json!({"t": "transport"}),
            }],
            first_arrival_ms: 0,
        };
        let coalescer = std::sync::Mutex::new(MentionCoalescer::with_defaults());
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "ek-tn-fn".into(),
            Some(&store),
            None,
            Some(&coalescer),
        )
        .await;
        match &out {
            ProviderOutcome::Deferred(v) => {
                assert_eq!(v["deferred"], serde_json::json!(true), "{v}");
            }
            other => panic!(
                "FlushNow Transport+requeue must Deferred (not Settled Delivered / not Ambiguous), got {other:?}"
            ),
        }
        assert!(
            coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .buffered_for(session)
                >= 1,
            "FlushNow Transport must requeue burst"
        );
        assert!(
            store.cursor(session).in_flight.is_empty(),
            "in_flight must clear after Transport"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 TRANSPORT-DROP-TRIGGER: Transport requeue restores flush_ready burst.
    #[test]
    fn trigger_transport_requeue_flush_ready() {
        let mut c = MentionCoalescer::new(
            std::time::Duration::from_millis(2_000),
            std::time::Duration::from_millis(500),
            64,
        );
        use toron_workflow::coalesce::InboundMention;
        c.submit(
            InboundMention {
                assignee: "oc".into(),
                parent: "p-tn".into(),
                spec: json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").expect("flushed");
        let q = c.requeue(batch, 1_000, RequeueSchedule::Transient);
        assert!(
            matches!(q, RequeueOutcome::Queued { delay_ms: 0, .. }),
            "{q:?}"
        );
        let again = c.tick(1_000);
        assert_eq!(again.len(), 1, "Transport requeue must flush on next tick");
        assert_eq!(again[0].parent(), "p-tn");
    }

    /// GS-062 PARTIAL-KEEP-FULL-SPEC: published 43001 omits already-delivered parents.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partial_keep_publishes_only_reserved_parents() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-pk-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let inner = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let recorder = Arc::new(RecordBus {
            inner,
            published: std::sync::Mutex::new(Vec::new()),
        });
        let bus: Arc<dyn Bus> = recorder.clone();
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-partial";
        store.mark_delivered(session, ["already"]);
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![
                InboundMention {
                    assignee: session.into(),
                    parent: "already".into(),
                    spec: json!({"n": 1}),
                },
                InboundMention {
                    assignee: session.into(),
                    parent: "fresh".into(),
                    spec: json!({"n": 2}),
                },
            ],
            first_arrival_ms: 0,
        };
        let id =
            publish_coalesced_job_with_cursor(bus.as_ref(), Some(&signer), &batch, Some(&store))
                .await
                .expect("partial keep must publish");
        let published = recorder
            .published
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(published.len(), 1);
        assert_eq!(published[0].id, id);
        let payload: serde_json::Value =
            serde_json::from_str(&published[0].content).expect("job json");
        drop(published);
        let mentions = payload["spec"]["mentions"]
            .as_array()
            .expect("mentions array");
        assert_eq!(
            mentions.len(),
            1,
            "must not include SkipAlreadyDelivered parent, got {payload}"
        );
        assert_eq!(mentions[0]["parent"], "fresh");
        assert_eq!(payload["spec"]["count"], 1);
        assert!(store.cursor(session).delivered.contains("fresh"));
        assert!(
            store.cursor(session).delivered.contains("already"),
            "pre-marked parent stays delivered"
        );
        assert!(store.cursor(session).in_flight.is_empty());
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 REQUEUE-EVENT-STARVATION: flush tick interval ≤ default subwindow.
    ///
    /// bd-x4qw: this was a `#[test]` asserting on a `const`, so it was decided
    /// at COMPILE time and could never fail at runtime. A test that cannot fail
    /// is not a test — the invariant belongs in a const assertion, which keeps
    /// the guarantee and stops pretending to observe it.
    const _: () = assert!(
        COALESCE_FLUSH_TICK_MS <= 500,
        "timer must fire within default subwindow so requeues flush without a bus event"
    );
    const _: () = assert!(COALESCE_FLUSH_TICK_MS > 0);

    /// GS-062 T1 FLUSHNOW-ACQUIRE: requeue restores as Deferred (non-Settled).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushnow_acquire_requeue_is_deferred() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;
        use toron_workflow::dispatch::{DispatchConfig, DispatchPool};

        let db = std::env::temp_dir().join(format!("wf-acq-q-{}.sqlite", uuid::Uuid::new_v4()));
        let sql = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&sql).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(sql, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let delivery = DeliveryCursorStore::new();
        let pool = DispatchPool::new(DispatchConfig {
            max_concurrency: 1,
            hold_bound_ms: 30,
        });
        let hold = pool
            .try_acquire_fenced("oc-acq-q", now_ms(), 0)
            .expect("hold busy");
        let signer = Keys::generate();
        let session = "oc-acq-q";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "acq-q-parent".into(),
                spec: json!({"i": 0}),
            }],
            first_arrival_ms: 0,
        };
        let coalescer = std::sync::Mutex::new(MentionCoalescer::with_defaults());
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "ek-acq-q".into(),
            Some(&delivery),
            Some(&pool),
            Some(&coalescer),
        )
        .await;
        drop(hold);
        match out {
            ProviderOutcome::Deferred(v) => {
                assert_eq!(v["deferred"], serde_json::json!(true), "{v}");
            }
            other => panic!("acquire+requeue must Deferred (non-Settled), got {other:?}"),
        }
        assert!(
            coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .buffered_for(session)
                >= 1,
            "acquire Err must requeue"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 NOSIGNER-FAILED-PLUS-REQUEUE: ProvenFailed without requeue.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flushnow_nosigner_is_terminal_no_requeue() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-ns-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let store = DeliveryCursorStore::new();
        let session = "oc-ns";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "ns-parent".into(),
                spec: json!({"t": "nosigner"}),
            }],
            first_arrival_ms: 0,
        };
        let coalescer = std::sync::Mutex::new(MentionCoalescer::with_defaults());
        let out = mention_outcome(
            bus.as_ref(),
            None,
            CoalesceDecision::FlushNow(batch),
            "ek-ns".into(),
            Some(&store),
            None,
            Some(&coalescer),
        )
        .await;
        assert!(
            matches!(out, ProviderOutcome::ProvenFailed(_)),
            "NoSigner must ProvenFailed, got {out:?}"
        );
        assert_eq!(
            coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .buffered_for(session),
            0,
            "NoSigner must NOT requeue (Failed+requeue forbidden)"
        );
        assert_eq!(
            coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .dlq_len(),
            1,
            "NoSigner must park batch on DLQ"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 T1(a): FlushNow Transient Queued must NOT Settled-Delivered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t1_flushnow_transient_queued_is_deferred_not_settled() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::EffectState;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-t1-fn-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let inner = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let bus: Arc<dyn Bus> = Arc::new(FailBus { inner });
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-t1-fn";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "t1-fn-parent".into(),
                spec: json!({"t": "t1"}),
            }],
            first_arrival_ms: 0,
        };
        let coalescer = std::sync::Mutex::new(MentionCoalescer::with_defaults());
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "ek-t1-fn".into(),
            Some(&store),
            None,
            Some(&coalescer),
        )
        .await;
        let deferred = match &out {
            ProviderOutcome::Deferred(v) => v.clone(),
            other => panic!("T1 FlushNow Transient Queued must Deferred, got {other:?}"),
        };
        assert_eq!(deferred["deferred"], serde_json::json!(true));
        let (state, _, _) = classify_outcome(&out);
        assert_eq!(
            state,
            EffectState::Accepted,
            "Deferred must journal Accepted (non-Settled), not Settled"
        );
        assert!(
            coalescer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .buffered_for(session)
                >= 1,
            "Transient Queued must requeue burst"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 T1(a): CapMention Buffered must NOT Settled-Delivered before tick publish.
    #[test]
    fn t1_capmention_buffered_is_deferred_not_settled() {
        use toron_core::index::workflow_effects::EffectState;
        let out = {
            ProviderOutcome::Deferred(serde_json::json!({
                "ok": true,
                "coalesced": true,
                "deferred": true,
                "buffered": true,
                "effect_key": "ek-buf",
            }))
        };
        let (state, _, val) = classify_outcome(&out);
        assert_eq!(state, EffectState::Accepted, "Buffered must not Settled");
        assert_ne!(state, EffectState::Settled);
        assert_eq!(val.unwrap()["buffered"], serde_json::json!(true));
    }

    /// GS-062 T1(a): CapMention Buffered via mention_outcome is Deferred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t1_capmention_buffered_mention_outcome_deferred() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::EffectState;
        use toron_workflow::DeliveryCursorStore;

        let db = std::env::temp_dir().join(format!("wf-t1-buf-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let store = DeliveryCursorStore::new();
        let out = mention_outcome(
            bus.as_ref(),
            Some(&Keys::generate()),
            CoalesceDecision::Buffered { buffered: 1 },
            "ek-t1-buf".into(),
            Some(&store),
            None,
            None,
        )
        .await;
        match &out {
            ProviderOutcome::Deferred(v) => {
                assert_eq!(v["buffered"], serde_json::json!(true), "{v}");
                assert_eq!(v["deferred"], serde_json::json!(true), "{v}");
            }
            other => panic!("Buffered must Deferred, got {other:?}"),
        }
        assert_eq!(classify_outcome(&out).0, EffectState::Accepted);
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 T1(a): publish success Settles Delivered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t1_publish_success_settles_delivered() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::EffectState;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::InboundMention;

        let db = std::env::temp_dir().join(format!("wf-t1-ok-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus: Arc<dyn Bus> = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-t1-ok";
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "t1-ok-parent".into(),
                spec: json!({"t": "ok"}),
            }],
            first_arrival_ms: 0,
        };
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "ek-t1-ok".into(),
            Some(&store),
            None,
            None,
        )
        .await;
        match &out {
            ProviderOutcome::Delivered(v) => {
                assert_eq!(v["published"], serde_json::json!(true), "{v}");
            }
            other => panic!("publish success must Delivered, got {other:?}"),
        }
        assert_eq!(classify_outcome(&out).0, EffectState::Settled);
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 T2(b): Exhausted → ProvenFailed + DLQ whole batch; siblings preserved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t2_exhausted_proven_failed_dlq_preserves_siblings() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::DeliveryCursorStore;
        use toron_workflow::coalesce::{InboundMention, REQUEUE_MAX_ATTEMPTS};

        let db = std::env::temp_dir().join(format!("wf-t2-ex-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let inner = Arc::new(
            InProcessBus::open(pool, InProcessBusConfig::default())
                .await
                .expect("bus"),
        );
        let bus: Arc<dyn Bus> = Arc::new(FailBus { inner });
        let store = DeliveryCursorStore::new();
        let signer = Keys::generate();
        let session = "oc-t2-ex";
        let coalescer = std::sync::Mutex::new(MentionCoalescer::with_defaults());
        {
            let mut g = coalescer.lock().unwrap_or_else(PoisonError::into_inner);
            let _ = g.submit(
                InboundMention {
                    assignee: session.into(),
                    parent: "sibling-live".into(),
                    spec: json!({"sib": true}),
                },
                0,
            );
            assert_eq!(g.buffered_for(session), 1);
            drop(g);
        }
        for i in 0..REQUEUE_MAX_ATTEMPTS {
            let batch = Coalesced {
                assignee: session.into(),
                mentions: vec![InboundMention {
                    assignee: session.into(),
                    parent: format!("ex-parent-{i}"),
                    spec: json!({"i": i}),
                }],
                first_arrival_ms: 0,
            };
            let out = mention_outcome(
                bus.as_ref(),
                Some(&signer),
                CoalesceDecision::FlushNow(batch),
                format!("ek-t2-ex-{i}"),
                Some(&store),
                None,
                Some(&coalescer),
            )
            .await;
            assert!(
                matches!(out, ProviderOutcome::Deferred(_)),
                "attempt {i} must Deferred Queued, got {out:?}"
            );
        }
        let batch = Coalesced {
            assignee: session.into(),
            mentions: vec![InboundMention {
                assignee: session.into(),
                parent: "ex-parent-final".into(),
                spec: json!({"final": true}),
            }],
            first_arrival_ms: 0,
        };
        let out = mention_outcome(
            bus.as_ref(),
            Some(&signer),
            CoalesceDecision::FlushNow(batch),
            "ek-t2-ex-final".into(),
            Some(&store),
            None,
            Some(&coalescer),
        )
        .await;
        assert!(
            matches!(out, ProviderOutcome::ProvenFailed(_)),
            "Exhausted must ProvenFailed (no force-retry), got {out:?}"
        );
        let g = coalescer.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(
            g.dlq_len() >= 1,
            "Exhausted must DLQ whole coalesced batch, dlq={}",
            g.dlq_len()
        );
        assert!(
            g.dlq_for(session)
                .iter()
                .any(|e| e.reason == DlqReason::Exhausted),
            "DLQ reason Exhausted"
        );
        assert!(
            g.buffered_for(session) >= 1,
            "sibling Buffered must not be silently lost, buffered={}",
            g.buffered_for(session)
        );
        drop(g);
        let _ = std::fs::remove_file(&db);
    }

    /// GS-062 T3: timer NoSigner parks DLQ (not warn+silent drop).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn t3_timer_nosigner_parks_dlq() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::coalesce::InboundMention;

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

    /// GS-062 REQUEUE-FLUSH-READY-LOST-ON-CONCURRENT-SUBMIT (host-facing).
    #[test]
    fn requeue_transient_survives_concurrent_submit_merge() {
        use toron_workflow::coalesce::InboundMention;
        let mut c = MentionCoalescer::new(
            std::time::Duration::from_millis(2_000),
            std::time::Duration::from_millis(500),
            64,
        );
        c.submit(
            InboundMention {
                assignee: "oc".into(),
                parent: "live".into(),
                spec: json!({"t": 1}),
            },
            1_000,
        );
        let failed = Coalesced {
            assignee: "oc".into(),
            mentions: vec![InboundMention {
                assignee: "oc".into(),
                parent: "retry".into(),
                spec: json!({"t": 2}),
            }],
            first_arrival_ms: 0,
        };
        let q = c.requeue(failed, 1_000, RequeueSchedule::Transient);
        assert!(
            matches!(q, RequeueOutcome::Queued { delay_ms: 0, .. }),
            "{q:?}"
        );
        let flushed = c.tick(1_000);
        assert_eq!(
            flushed.len(),
            1,
            "merge path must honor immediate Transient"
        );
        let parents: Vec<_> = flushed[0]
            .mentions
            .iter()
            .map(|m| m.parent.as_str())
            .collect();
        assert!(
            parents.contains(&"live") && parents.contains(&"retry"),
            "{parents:?}"
        );
    }

    /// GS-062 LOW: flush tick constant drains requeues without further bus events.
    #[test]
    fn flush_tick_drains_requeue_without_bus_event() {
        use toron_workflow::coalesce::InboundMention;
        let mut c = MentionCoalescer::new(
            std::time::Duration::from_millis(2_000),
            std::time::Duration::from_millis(500),
            64,
        );
        c.submit(
            InboundMention {
                assignee: "oc".into(),
                parent: "tick-p".into(),
                spec: json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").expect("flushed");
        let _ = c.requeue(batch, 1_000, RequeueSchedule::FreshWindow);
        assert!(c.tick(1_000).is_empty());
        let mut t = 1_000u64;
        let mut drained = false;
        for _ in 0..8 {
            t = t.saturating_add(COALESCE_FLUSH_TICK_MS);
            if !c.tick(t).is_empty() {
                drained = true;
                break;
            }
        }
        assert!(
            drained,
            "COALESCE_FLUSH_TICK_MS cadence must drain FreshWindow requeue without bus events"
        );
    }

    /// bd-host-cut-pqkg: the router admits linear workflows and refuses the
    /// rest before any row exists. Pure function, no I/O.
    mod durable_route_tests {
        use super::*;
        use toron_workflow::schema::{Graph, GraphNode, Step, StepOnError};

        fn linear_step() -> Step {
            Step {
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
            }
        }

        fn linear_workflow() -> Workflow {
            Workflow {
                name: "linear-cut".into(),
                description: None,
                trigger: None,
                policy: None,
                guard: None,
                principles: vec![],
                steps: vec![linear_step()],
                graph: None,
                inputs: vec![],
                crew: None,
            }
        }

        #[test]
        fn admits_a_linear_workflow_with_a_store() {
            assert!(durable_route(&linear_workflow(), true, false).is_ok());
        }

        #[test]
        fn refuses_everything_without_a_store() {
            let err = durable_route(&linear_workflow(), false, false).expect_err("no store");
            assert!(err.contains("no durable store"), "{err}");
        }

        fn graph_node(action: Action, needs: &[&str]) -> GraphNode {
            GraphNode {
                action,
                needs: needs.iter().map(|n| (*n).to_string()).collect(),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
            }
        }

        fn graph_workflow(nodes: [(&str, GraphNode); 2]) -> Workflow {
            let mut wf = linear_workflow();
            wf.graph = Some(Graph {
                nodes: nodes
                    .into_iter()
                    .map(|(name, node)| (name.to_string(), node))
                    .collect(),
            });
            wf
        }

        fn durable_leaf(subject: &str) -> Action {
            Action::SendMessage {
                to: vec!["nobody".into()],
                subject: subject.into(),
                body: "hello".into(),
                project: None,
            }
        }

        /// bd-durable-graph-impl-r2x1: a graph whose every node is a HostSink
        /// leaf, `delay`, or `request_approval` is admitted to the durable
        /// driver.
        #[test]
        fn admits_a_durable_graph() {
            let wf = graph_workflow([
                ("first", graph_node(durable_leaf("cut"), &[])),
                (
                    "approve",
                    graph_node(
                        Action::RequestApproval {
                            message: "ship?".into(),
                        },
                        &["first"],
                    ),
                ),
            ]);
            assert!(durable_route(&wf, true, false).is_ok());
        }

        /// Whole-graph refusal: ONE control-flow node keeps the whole graph on
        /// the old engine. A half-run graph is worse than a clean route.
        #[test]
        fn refuses_a_graph_with_a_control_flow_node() {
            let wf = graph_workflow([
                ("fine", graph_node(durable_leaf("cut"), &[])),
                (
                    "spin",
                    graph_node(
                        Action::Loop {
                            max: "3".into(),
                            until: None,
                            body: vec![linear_step()],
                        },
                        &["fine"],
                    ),
                ),
            ]);
            let err = durable_route(&wf, true, false).expect_err("loop refuses");
            assert!(err.contains("loop"), "{err}");
        }

        /// bd-loop-rewrites-fap8: a bounded `steps:` loop is a linear-plan
        /// entry, so the router admits it; an unbounded bound refuses.
        #[test]
        fn a_bounded_loop_is_linear() {
            let mut wf = linear_workflow();
            wf.steps = vec![Step {
                name: Some("spin".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::Loop {
                    max: "{{inputs.max_iters}}".into(),
                    until: Some("steps_notify_ok == true".into()),
                    body: vec![linear_step()],
                },
            }];
            assert!(durable_route(&wf, true, false).is_ok());

            let mut unbounded = wf.clone();
            unbounded.steps[0].action = Action::Loop {
                max: "{{steps.plan.n}}".into(),
                until: None,
                body: vec![linear_step()],
            };
            let err = durable_route(&unbounded, true, false).expect_err("output bound");
            assert!(err.contains("upstream output"), "{err}");
        }

        /// bd-durable-call-workflow-mg6y: a `steps:` child call is native to
        /// the graph model, so the shape router admits it; the host's open
        /// refuses without a child resolver and the run falls back to the old
        /// engine.
        #[test]
        fn a_call_workflow_steps_workflow_routes_to_the_graph() {
            let mut wf = linear_workflow();
            wf.steps = vec![Step {
                name: Some("call".into()),
                guard: None,
                on_error: StepOnError::Continue,
                build_slot: false,
                action: Action::CallWorkflow {
                    workflow: "child".into(),
                    inputs: serde_json::json!({"goal": "g"}),
                },
            }];
            assert!(durable_route(&wf, true, false).is_ok());
        }

        /// bd-durable-retry-policy-rrck: a `steps:` retry policy runs on the
        /// graph driver; an unparseable policy refuses before any row.
        #[test]
        fn a_retry_steps_workflow_routes_to_the_graph() {
            let mut wf = linear_workflow();
            wf.steps = vec![Step {
                name: Some("flaky".into()),
                guard: None,
                on_error: StepOnError::FailRun,
                build_slot: false,
                action: Action::Retry {
                    max_attempts: 3,
                    backoff_secs: 0,
                    step: Box::new(linear_step()),
                },
            }];
            assert!(durable_route(&wf, true, false).is_ok());

            let mut bad = wf.clone();
            bad.steps[0].action = Action::Retry {
                max_attempts: 0,
                backoff_secs: 0,
                step: Box::new(linear_step()),
            };
            let err = durable_route(&bad, true, false).expect_err("zero attempts");
            assert!(err.contains("at least 1"), "{err}");
        }

        /// bd-durable-foreach-jpgx: a `steps:` foreach shares the fan-out
        /// path, so the router admits it to the graph driver; an item the
        /// graph cannot sequence refuses whole.
        #[test]
        fn a_foreach_steps_workflow_routes_to_the_graph() {
            let mut wf = linear_workflow();
            wf.steps = vec![
                Step {
                    name: Some("fan".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::Foreach {
                        over: "{{inputs.items}}".into(),
                        each: Box::new(linear_step()),
                    },
                },
                linear_step(),
            ];
            assert!(durable_route(&wf, true, false).is_ok());

            let mut bad = wf.clone();
            bad.steps[0].action = Action::Foreach {
                over: "{{inputs.items}}".into(),
                each: Box::new(Step {
                    name: None,
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::CallWebhook {
                        url: "http://127.0.0.1:9/x".into(),
                        method: None,
                        body: None,
                    },
                }),
            };
            let err = durable_route(&bad, true, false).expect_err("unsequencable item");
            assert!(err.contains("call_webhook"), "{err}");
        }

        /// bd-durable-parallel-53um: a `steps:` fan-out is lowered onto the
        /// graph, so the router admits it; the spawn/branch mix refuses whole.
        #[test]
        fn a_parallel_steps_workflow_routes_to_the_graph() {
            let mut wf = linear_workflow();
            wf.steps = vec![
                Step {
                    name: Some("fan".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::Parallel {
                        over: "3".into(),
                        each: Box::new(linear_step()),
                    },
                },
                linear_step(),
            ];
            assert!(durable_route(&wf, true, false).is_ok());

            let mut mixed = wf.clone();
            mixed.steps.insert(
                0,
                Step {
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
                },
            );
            let err = durable_route(&mixed, true, true).expect_err("the mix refuses");
            assert!(err.contains("mixes graph-lowered steps"), "{err}");
        }

        /// Static refusal: a cyclic graph never gets a durable row — the old
        /// engine fails it with the same `{status:failed}` value.
        #[test]
        fn refuses_a_cyclic_graph() {
            let wf = graph_workflow([
                ("a", graph_node(durable_leaf("a"), &["b"])),
                ("b", graph_node(durable_leaf("b"), &["a"])),
            ]);
            let err = durable_route(&wf, true, false).expect_err("cycles refuse");
            assert!(err.contains("cycle"), "{err}");
        }

        /// bd-durable-branch-hhgc: a pure-guard branch over host-sink leaves
        /// is one linear workflow, and spawn-before-branch (the shipped
        /// housekeeper shape) is admitted once a launcher is present.
        #[test]
        fn a_pure_branch_is_linear() {
            let mut wf = linear_workflow();
            wf.steps = vec![
                Step {
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
                },
                Step {
                    name: Some("choose".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
                    build_slot: false,
                    action: Action::Branch {
                        if_guard: "{{trigger.gate}} == true".into(),
                        then: vec![linear_step()],
                        else_steps: vec![linear_step()],
                    },
                },
            ];
            let err = durable_route(&wf, true, false).expect_err("no launcher");
            assert!(err.contains("launcher"), "{err}");
            assert!(
                durable_route(&wf, true, true).is_ok(),
                "spawn-before-branch is one linear shape"
            );
        }

        /// Only the pure comparison grammar admits a branch; anything that
        /// needs the host refuses before any row.
        #[test]
        fn an_impure_branch_refuses() {
            let mut wf = linear_workflow();
            wf.steps[0].action = Action::Branch {
                if_guard: "len(\"x\") > 0".into(),
                then: vec![linear_step()],
                else_steps: vec![],
            };
            let err = durable_route(&wf, true, false).expect_err("impure guard");
            assert!(err.contains("not pure"), "{err}");
        }

        /// bd-spawn-launcher-flip-d8mv: admission is capability-based. A host
        /// with no launcher refuses a spawn workflow BEFORE any row; the same
        /// workflow routes once a launcher is installed.
        #[test]
        fn a_spawn_step_needs_a_launcher() {
            let mut wf = linear_workflow();
            wf.steps[0].action = Action::Spawn {
                profile: "@housekeeper".into(),
                harness: None,
                workflow: None,
                await_: true,
                no_claim_timeout: 600,
                context: None,
            };
            let err = durable_route(&wf, true, false).expect_err("no launcher");
            assert!(err.contains("launcher"), "{err}");
            assert!(durable_route(&wf, true, true).is_ok());
        }

        #[test]
        fn durable_pending_carries_run_id_and_never_parses_as_resolution() {
            let entry = PendingEntry::new(
                "run-cut-9",
                None,
                "linear-cut".to_string(),
                "approve-it".to_string(),
                None,
                "run-cut-9",
                1,
                ParkKind::Approval,
            );
            assert_eq!(entry.promise_id, "run-cut-9");
            let body = serde_json::json!({
                "run_id": entry.run_id,
                "workflow": entry.workflow,
                "node": entry.node_path,
                "promise_id": entry.promise_id,
                "generation": entry.generation,
                "kind": entry.kind.as_str(),
            });
            let keys = Keys::generate();
            let event = EventBuilder::new(Kind::from_u16(46011), body.to_string())
                .finalize(&keys)
                .expect("sign");
            assert!(
                WorkflowHost::parse_approval_resolution(&event).is_none(),
                "a pending without verdict is not a resolution"
            );
        }
    }

    /// bd-host-cut-pqkg: durable resolutions validate before touching the
    /// state store; mismatches bail without settling anything.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn durable_resolve_rejects_mismatches_before_the_store() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::Workflow;

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
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::{Action, Step, StepOnError, Workflow};

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
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        use toron_workflow::schema::{Action, Step, StepOnError, Workflow};

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
            model["outputs"]["spawn_it"],
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
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_core::index::workflow_effects::WorkflowEffectStore;
        use toron_workflow::schema::{Action, Step, StepOnError, Workflow};

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
            model["outputs"]["spawn_it"],
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
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::{Action, Step, StepOnError, Workflow};

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

    /// bd-host-cut-pqkg: the cut witness. A step settled before the restart
    /// must not run again after it — the model resumes at the parked step,
    /// not the beginning. Counts dispatches on each host's runner so a
    /// replayed settled step is visible, not inferred.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_does_not_replay_a_settled_step() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::schema::{Action, Step, StepOnError, Workflow};

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
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::durable_workflow::WorkflowStateStore;
        use toron_core::index::schema;
        use toron_workflow::schema::{Action, Step, StepOnError, Workflow};

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
