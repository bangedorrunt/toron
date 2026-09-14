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
//! - The engine's ActionRunner dispatches through the Mailbox (a DM send
//!   emits a kind-14 gift wrap) and STAMPS every event it emits with
//!   `toron:workflow-emitted` — so a workflow can never re-trigger itself.
//! - `@mention` dispatch runs through a [`MentionCoalescer`] (one job per
//!   burst), ticked by the trigger loop's per-event flush.
//!
//! governed-by: ADR-0004 ships 1/2/4/5, ADR-0005 Spec 2

use std::sync::Arc;

use anyhow::{Context, Result};
use nostr::prelude::{Event, EventBuilder, EventId, FinalizeEvent, Keys, Kind, Tag, Timestamp};
use toron_core::bus::{Bus, SubscriptionEvent};
use toron_core::mailbox::{Mailbox, SendMessage, UnwrappedMail};
use toron_workflow::coalesce::{CoalesceDecision, Coalesced, MentionCoalescer};
use toron_workflow::engine::{
    ActionInvocation, ActionRunner, CleanupKind, CleanupRequest, Decision, DispatchPlan,
    EffectJournalMode, EffectRunner, EffectView, FORCED_CLEANUP_CAPABILITY, ParkKind, PendingEntry,
    PendingRegistry, ProviderCapability, ProviderErrorClass, ProviderOutcome, Resolution,
    ResolutionMismatch, ResolutionVerdict, RunInput, RunJournal, RunPhase, RunTransition,
    TerminalLedger, TerminalNote, TerminalRecord, UnknownReport, WorkflowEngine, attribute_parks,
    capability_for_action_type, classify_provider_error_msg, cleanup_effect_key, corr_for_effect,
    derive_effect_key, describe_unknown, invoke, plan_attempt, terminal_digest,
    validate_resolution,
};
use toron_workflow::jobs::{JobRequest, build_job_request_with_tags};
use toron_workflow::schema::{Action, Trigger, Workflow};
use toron_workflow::stamp_workflow_emitted;
use toron_workflow::trigger_filter::{
    matches_trigger, should_evaluate_trigger_event, trigger_payload, trigger_run_id,
};
use tracing::{debug, warn};

/// Drive an async future from the sync ActionRunner without nested-runtime
/// panics (ADR-0007 F8).
///
/// Always runs on a **dedicated OS thread** with its own current-thread
/// runtime. Calling `Handle::block_on` / dropping a Runtime from inside an
/// existing Tokio context panics; isolating the runner I/O is the only
/// correct fix for a sync `ActionRunner` over async Mailbox/Bus APIs.
fn block_on_async<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("build runner runtime")?;
        rt.block_on(fut)
    })
    .join()
    .map_err(|_| anyhow::anyhow!("runner thread panicked"))?
}

/// A production [`ActionRunner`]: dispatches a workflow action through the
/// Mailbox and stamps every emitted event with the `toron:workflow-emitted`
/// tag (bd-pkc part 3 completion — the loop-prevention belt is dead until
/// stamped).
///
/// Dispatch semantics (bd-i6kc, ADR-0032 Spec 1 + Spec 2.1 decision):
/// every call is partitioned by [`toron_workflow::engine::partition`] first —
/// - `SendMessage` / `SendDm` → `Mailbox::send_message` (kind-14 gift wrap).
/// - `DispatchJob` → a coalesced job via the [`MentionCoalescer`] (kind-43001
///   with the merged spec; the coalescer decides when to flush).
/// - Engine-owned control actions (`Delay` / `RequestApproval` / `Retry` /
///   `Branch` / `Foreach` / `Parallel` / `CallWorkflow` / `Spawn`) →
///   `control_action_misdispatch` failure (the engine sequences these; the
///   runner never executes them).
/// - `Verify` → `missing_sink` failure (verification runs through the
///   embedding-owned `VerifySink`, never this mailbox-only runner).
/// - `React` / `CallWebhook` → DEPRECATED (bd-i6kc retain-or-remove
///   decision: remove). No reaction/webhook sink is wired, and the webhook
///   SSRF contract (scheme/host policy, redirect and DNS-rebinding control,
///   bounds, redaction) is unmet, so both fail closed with
///   `unsupported_action` — a run using them fails the step, never silently
///   succeeds.
///
/// No arm returns bare `Null`: dispatch results are output-normalized, so a
/// null result is an `invalid_output` failure, not success.
///
/// The runner is sync (engine contract) and thread-safe; the coalescer is
/// parked behind a `Mutex`.
pub fn mailbox_runner(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
) -> ActionRunner {
    mailbox_runner_with_dispatcher(
        mailbox,
        coalescer,
        job_signer,
        Arc::new(std::sync::Mutex::new(None)),
    )
}

/// Stable compat effect identity (bd-1kr2): the context-free runner has
/// no run/node scope (run-scoped keys arrive with bd-zczp's RunContext),
/// so the key is `compat/{action_type}/{sha256(canonical action JSON)}`.
/// Same payload retries attach to one journaled effect; a different
/// payload mints a different key. Returns `(effect_key, action_digest)`.
///
/// Boundary (documented over-dedupe): two unrelated runs sending
/// byte-identical payloads share a key at this layer — the second
/// attaches to the first's stored outcome instead of re-sending.
/// bd-zczp removes this by scoping keys with the run context.
pub(crate) fn compat_effect_key(action: &Action) -> (String, String) {
    use sha2::{Digest, Sha256};
    let canonical = serde_json::to_value(action).expect("Action is JSON-serializable");
    let canonical = serde_json::to_string(&canonical).expect("canonical action JSON serializes");
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hex::encode(hasher.finalize());
    let key = format!("compat/{}/{}", action.type_name(), digest);
    (key, digest)
}

/// Build the fenced dispatch request for one compat action (bd-1kr2).
/// Transport `corr` == effect key (Spec 5.4); the capability is the
/// conservative journal default for the action type (mailbox/coalescer
/// providers offer no query-or-idempotency contract, so uncertainty
/// refuses a blind retry instead of risking a duplicate send).
pub(crate) fn compat_request(action: &Action) -> DispatchRequest {
    let (effect_key, action_digest) = compat_effect_key(action);
    let action_type = action.type_name().to_string();
    let project = match action {
        Action::SendMessage { project, .. } | Action::SendDm { project, .. } => project.clone(),
        _ => None,
    };
    DispatchRequest {
        effect_key,
        run_id: "compat".to_string(),
        project,
        workflow_digest: "compat".to_string(),
        node_path: action_type.clone(),
        iteration_key: None,
        action_type: action_type.clone(),
        action_digest,
        redacted_action: None,
        capability: capability_for_action_type(&action_type),
    }
}

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

/// Map one coalescer decision onto a provider outcome (bd-zczp): Buffered
/// records the burst-buffer hand-off; FlushNow publishes with typed
/// errors — NoSigner is proven never-sent, transport loss is ambiguous.
fn mention_outcome(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    decision: CoalesceDecision,
    key: String,
) -> ProviderOutcome {
    match decision {
        CoalesceDecision::Buffered { .. } => ProviderOutcome::Delivered(serde_json::json!({
            "ok": true,
            "coalesced": true,
            "effect_key": key,
        })),
        CoalesceDecision::FlushNow(coalesced) => {
            match publish_coalesced_job(bus, signer, &coalesced) {
                Ok(id) => ProviderOutcome::Delivered(serde_json::json!({
                    "ok": true,
                    "coalesced": false,
                    "published": true,
                    "job_id": id.to_hex(),
                    "effect_key": key,
                })),
                Err(CoalescePublishError::NoSigner) => {
                    ProviderOutcome::ProvenFailed("no job signer configured".to_string())
                }
                Err(CoalescePublishError::Transport(e)) => ProviderOutcome::Ambiguous(e),
            }
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
/// A production [`ActionRunner`] with fenced effect dispatch (bd-1kr2):
/// the shared dispatcher cell is read per call, so a late
/// [`WorkflowHost::with_effect_store`] attach rewires sends without a
/// rebuild. When the cell holds a dispatcher, the SendMessage / SendDm /
/// DispatchJob arms journal intent before invoking the provider
/// (claim → invoke → settle_with_fence → link_transport_corr, Spec 5.3)
/// and project `corr == effect_key` on the wire (Spec 5.4). When it
/// holds `None` the arms behave exactly as [`mailbox_runner`] (direct
/// sends, compat fallback for embeddings without a journal).
pub fn mailbox_runner_with_dispatcher(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
) -> ActionRunner {
    Arc::new(move |action: &Action| {
        // bd-i6kc: the context-free runner is the migration compat adapter —
        // every call crosses the boundary partition first. `None` from the
        // host closure means the required named sink is absent here.
        let value = toron_workflow::engine::invoke_compat(action, |action| match action {
            Action::SendMessage {
                to,
                subject,
                body,
                project,
            } => {
                let project = project.clone().unwrap_or_default();
                let mut tags = stamp_workflow_emitted(vec![]);
                let mut msg = SendMessage {
                    project_slug: project.clone(),
                    sender_name: sender_for(&project).to_string(),
                    to: to.clone(),
                    subject: subject.clone(),
                    body_md: body.clone(),
                    cc: vec![],
                    bcc: vec![],
                    importance: None,
                    ack_required: false,
                    thread_id: None,
                    sender_token: None,
                    overseer: false,
                    // ADR-0007 F6: every workflow-originated rumor carries the
                    // `toron:workflow-emitted` stamp (gift wrap stays
                    // metadata-hiding — the stamp is INSIDE the rumor).
                    extra_tags: vec![],
                };
                let disp = dispatcher.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let Some(disp) = disp else {
                    msg.extra_tags = tags;
                    let mb = mailbox.clone();
                    return Some(
                        match block_on_async(async move { mb.send_message(&msg).await }) {
                            Ok(res) => Ok(serde_json::json!({
                                "ok": true,
                                "count": res.count,
                            })),
                            Err(e) => {
                                Ok(serde_json::json!({ "ok": false, "error": e.to_string() }))
                            }
                        },
                    );
                };
                // bd-1kr2: fenced send — corr == effect_key (Spec 5.4).
                let req = compat_request(action);
                tags.push(Tag::parse(["corr", req.effect_key.as_str()]).expect("corr tag parses"));
                msg.extra_tags = tags;
                let mb = mailbox.clone();
                let key = req.effect_key.clone();
                let key_out = key.clone();
                Some(Ok(fenced_outcome(
                    &key_out,
                    block_on_async(async move {
                        disp.dispatch(&req, move |_| {
                            let sent =
                                match block_on_async(async move { mb.send_message(&msg).await }) {
                                    Ok(res) => Ok(res.count),
                                    Err(e) => Err(e.to_string()),
                                };
                            mail_outcome(sent, key)
                        })
                        .await
                    }),
                )))
            }
            Action::SendDm { to, body, project } => {
                let project = project.clone().unwrap_or_default();
                let mut tags = stamp_workflow_emitted(vec![]);
                let mut msg = SendMessage {
                    project_slug: project.clone(),
                    sender_name: sender_for(&project).to_string(),
                    to: vec![to.clone()],
                    subject: "workflow dm".to_string(),
                    body_md: body.clone(),
                    cc: vec![],
                    bcc: vec![],
                    importance: None,
                    ack_required: false,
                    thread_id: None,
                    sender_token: None,
                    overseer: false,
                    // ADR-0007 F6: workflow-originated DM rumors are stamped
                    // too — a workflow can never re-trigger itself.
                    extra_tags: vec![],
                };
                let disp = dispatcher.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let Some(disp) = disp else {
                    msg.extra_tags = tags;
                    let mb = mailbox.clone();
                    return Some(
                        match block_on_async(async move { mb.send_message(&msg).await }) {
                            Ok(res) => Ok(serde_json::json!({
                                "ok": true,
                                "count": res.count,
                            })),
                            Err(e) => {
                                Ok(serde_json::json!({ "ok": false, "error": e.to_string() }))
                            }
                        },
                    );
                };
                // bd-1kr2: fenced DM — corr == effect_key (Spec 5.4).
                let req = compat_request(action);
                tags.push(Tag::parse(["corr", req.effect_key.as_str()]).expect("corr tag parses"));
                msg.extra_tags = tags;
                let mb = mailbox.clone();
                let key = req.effect_key.clone();
                let key_out = key.clone();
                Some(Ok(fenced_outcome(
                    &key_out,
                    block_on_async(async move {
                        disp.dispatch(&req, move |_| {
                            let sent =
                                match block_on_async(async move { mb.send_message(&msg).await }) {
                                    Ok(res) => Ok(res.count),
                                    Err(e) => Err(e.to_string()),
                                };
                            mail_outcome(sent, key)
                        })
                        .await
                    }),
                )))
            }
            Action::DispatchJob { assignee, spec, .. } => {
                // @mention dispatch through the coalescer: one job per burst.
                let mention = toron_workflow::coalesce::InboundMention {
                    assignee: assignee.clone(),
                    parent: String::new(),
                    spec: serde_json::to_value(spec).expect("DispatchSpec is JSON-serializable"),
                };
                let now = now_ms();
                // P1 (bd-p1-poison-on-panic-audit-jwpd): recover the std
                // Mutex poison — a panic inside the guard must not brick
                // every subsequent flush (panic-in-panic = dead service).
                // P1 (F3.COALESCER-LOCK, batch-fix): the guard is scoped to
                // submit ONLY — the coalesced job is owned, the guard drops,
                // THEN the blocking publish runs (no lock held across
                // publish_coalesced_job's rt.block_on).
                let decision = {
                    let mut c = coalescer.lock().unwrap_or_else(|e| e.into_inner());
                    c.submit(mention, now)
                };
                let bus = mailbox.bus().clone();
                let signer = job_signer.clone();
                let disp = dispatcher.lock().unwrap_or_else(|e| e.into_inner()).clone();
                let Some(disp) = disp else {
                    return Some(match decision {
                        // Hard-cap flush: publish the coalesced 43001 NOW (the
                        // host's tick cannot — the buffer is already drained).
                        CoalesceDecision::FlushNow(coalesced) => {
                            let published =
                                publish_coalesced_job(bus.as_ref(), signer.as_ref(), &coalesced);
                            // bd-cnr5: an unpublished flush is a dropped effect —
                            // report ok:false loud, never ok:true with
                            // published:false. bd-zczp: a bus failure names
                            // itself (ambiguous) instead of borrowing the
                            // no-signer message.
                            match published {
                                Ok(_) => Ok(serde_json::json!({
                                    "ok": true,
                                    "coalesced": false,
                                    "published": true,
                                    "error": serde_json::Value::Null,
                                })),
                                Err(CoalescePublishError::NoSigner) => Ok(serde_json::json!({
                                    "ok": false,
                                    "coalesced": false,
                                    "published": false,
                                    "error": "no job signer configured",
                                })),
                                Err(CoalescePublishError::Transport(e)) => Ok(serde_json::json!({
                                    "ok": false,
                                    "coalesced": false,
                                    "published": false,
                                    "error": e,
                                })),
                            }
                        }
                        CoalesceDecision::Buffered { .. } => Ok(serde_json::json!({
                            "ok": true,
                            "coalesced": true,
                        })),
                    });
                };
                // bd-1kr2: fenced @mention hand-off. Buffered means the
                // burst buffer accepted the mention (the host tick owns the
                // later 43001 publish — Settled here records the hand-off,
                // not the wire delivery; the in-memory coalescer's
                // crash-loss window is unchanged and documented).
                // A coalesced 43001 merges N effects into one job, so no
                // single corr rides it — per-effect corr lives in the
                // journal's transport_corr projection (linked by dispatch).
                let req = compat_request(action);
                let key = req.effect_key.clone();
                let key_out = key.clone();
                Some(Ok(fenced_outcome(
                    &key_out,
                    block_on_async(async move {
                        disp.dispatch(&req, move |_| {
                            mention_outcome(bus.as_ref(), signer.as_ref(), decision, key)
                        })
                        .await
                    }),
                )))
            }
            // Every other HostSink action (today: `Verify`) has no sink on
            // this mailbox-only runner — the boundary reports `missing_sink`.
            _ => None,
        });
        Ok(value)
    })
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

/// The engine-facing run-lifecycle journal over this store (bd-mi9n).
///
/// Sync by the engine's house rule ([`EffectRunner`] is a sync closure too):
/// the durable write runs on `block_on_async`'s isolated thread. Best-effort —
/// a journal failure is reported and never changes the run's outcome.
///
/// The engine reports only the four phases it can prove (Started, Running,
/// Succeeded, Failed); host-level states (Suspended, Blocked, Cancelled) stay
/// the host's own calls through [`apply_run_status`].
pub fn run_journal(store: WorkflowEffectStore) -> RunJournal {
    Arc::new(move |t: &RunTransition| {
        let target = match t.phase {
            RunPhase::Started | RunPhase::Running => RunStatus::Running,
            RunPhase::Succeeded => RunStatus::Succeeded,
            RunPhase::Failed => RunStatus::Failed,
        };
        let store = store.clone();
        let (run_id, workflow, digest) = (
            t.run_id.clone(),
            t.workflow.clone(),
            t.workflow_digest.clone(),
        );
        let label = run_id.clone();
        if let Err(e) = block_on_async(async move {
            apply_run_status(&store, &run_id, &workflow, &digest, target).await
        }) {
            eprintln!(
                "toron-mcp: run lifecycle row for {label} ({:?}) not journaled: {e:#}",
                t.phase
            );
        }
    })
}

pub fn mailbox_effect_runner(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
    dispatcher: Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
) -> EffectRunner {
    Arc::new(move |inv: &ActionInvocation| {
        let outcome = invoke(inv, |action| {
            match action {
                Action::SendMessage {
                    to,
                    subject,
                    body,
                    project,
                } => {
                    let project = project.clone().unwrap_or_default();
                    let (key, digest) = scoped_effect_key(inv);
                    let mut tags = stamp_workflow_emitted(vec![]);
                    tags.push(Tag::parse(["corr", key.as_str()]).expect("corr tag parses"));
                    let msg = SendMessage {
                        project_slug: project.clone(),
                        sender_name: sender_for(&project).to_string(),
                        to: to.clone(),
                        subject: subject.clone(),
                        body_md: body.clone(),
                        cc: vec![],
                        bcc: vec![],
                        importance: None,
                        ack_required: false,
                        thread_id: None,
                        sender_token: None,
                        overseer: false,
                        extra_tags: tags,
                    };
                    let req = scoped_request(inv, digest, key.clone());
                    send_via_dispatch(mailbox.clone(), &dispatcher, req, msg, key)
                }
                Action::SendDm { to, body, project } => {
                    let project = project.clone().unwrap_or_default();
                    let (key, digest) = scoped_effect_key(inv);
                    let mut tags = stamp_workflow_emitted(vec![]);
                    tags.push(Tag::parse(["corr", key.as_str()]).expect("corr tag parses"));
                    let msg = SendMessage {
                        project_slug: project.clone(),
                        sender_name: sender_for(&project).to_string(),
                        to: vec![to.clone()],
                        subject: "workflow dm".to_string(),
                        body_md: body.clone(),
                        cc: vec![],
                        bcc: vec![],
                        importance: None,
                        ack_required: false,
                        thread_id: None,
                        sender_token: None,
                        overseer: false,
                        extra_tags: tags,
                    };
                    let req = scoped_request(inv, digest, key.clone());
                    send_via_dispatch(mailbox.clone(), &dispatcher, req, msg, key)
                }
                Action::DispatchJob { assignee, spec, .. } => {
                    let mention = toron_workflow::coalesce::InboundMention {
                        assignee: assignee.clone(),
                        parent: String::new(),
                        spec: serde_json::to_value(spec)
                            .expect("DispatchSpec is JSON-serializable"),
                    };
                    let now = now_ms();
                    let decision = {
                        let mut c = coalescer.lock().unwrap_or_else(|e| e.into_inner());
                        c.submit(mention, now)
                    };
                    let (key, digest) = scoped_effect_key(inv);
                    let req = scoped_request(inv, digest, key.clone());
                    let bus = mailbox.bus().clone();
                    let signer = job_signer.clone();
                    let disp = dispatcher.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    let Some(disp) = disp else {
                        // No journal: direct hand-off; failures return Err
                        // so `invoke` classifies them (I5) identically.
                        return match mention_outcome(bus.as_ref(), signer.as_ref(), decision, key) {
                            ProviderOutcome::Delivered(v) => Ok(v),
                            ProviderOutcome::ProvenFailed(m) | ProviderOutcome::Ambiguous(m) => {
                                Err(anyhow::anyhow!(m))
                            }
                        };
                    };
                    let key_out = key.clone();
                    Ok(fenced_outcome(
                        &key_out,
                        block_on_async(async move {
                            disp.dispatch(&req, move |_| {
                                mention_outcome(bus.as_ref(), signer.as_ref(), decision, key)
                            })
                            .await
                        }),
                    ))
                }
                // `verify` (bd-engine-run-path-journaling-fwlx): the engine
                // already executed the embedding's `VerifySink` and handed the
                // observed result over on the invocation (ADR-0024 Spec 2), so
                // this journals an effect that already happened — the provider
                // re-runs nothing. Journaling here is what keeps the host's
                // Enforced contract honest: without it a sink this runner did
                // not cover would perform a real command with NO journal row.
                Action::Verify { .. } => {
                    // No `engine.verify_sink()` is attached on this host, so the
                    // observed value is normally the sink's own
                    // `{ok:false, error:"verify sink missing"}` — a typed
                    // failure naming the real cause, not a generic one.
                    let observed = inv.run.provider_result.clone().unwrap_or_else(
                        || serde_json::json!({ "ok": false, "error": "verify sink missing" }),
                    );
                    let (key, digest) = scoped_effect_key(inv);
                    let req = scoped_request(inv, digest, key.clone());
                    let disp = dispatcher.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    let Some(disp) = disp else {
                        // No journal attached: hand the observation straight
                        // through; `invoke` classifies it (I5) identically.
                        return Ok(observed);
                    };
                    let key_out = key.clone();
                    Ok(fenced_outcome(
                        &key_out,
                        block_on_async(async move {
                            disp.dispatch(&req, move |_| verify_outcome(observed)).await
                        }),
                    ))
                }
                // Any other HostSink has no sink on this mailbox-only runner —
                // fail loud, never silently succeed.
                _ => Err(anyhow::anyhow!(
                    "effect runner has no sink for '{}'",
                    action.type_name()
                )),
            }
        });
        Ok(match outcome {
            Ok(o) => o.value,
            Err(f) => f.to_value(),
        })
    })
}

/// Send one fenced mail message: journaled dispatch when a dispatcher is
/// attached, direct classified send otherwise (bd-zczp). Direct-path
/// failures return `Err` so [`invoke`] classifies them (I5) identically.
fn send_via_dispatch(
    mailbox: Arc<Mailbox>,
    dispatcher: &Arc<std::sync::Mutex<Option<EffectDispatcher>>>,
    req: DispatchRequest,
    msg: SendMessage,
    key: String,
) -> anyhow::Result<serde_json::Value> {
    let disp = dispatcher.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let Some(disp) = disp else {
        return match block_on_async(async move { mailbox.send_message(&msg).await }) {
            Ok(res) => Ok(serde_json::json!({ "ok": true, "count": res.count })),
            Err(e) => Err(anyhow::anyhow!(e.to_string())),
        };
    };
    let key_out = key.clone();
    Ok(fenced_outcome(
        &key_out,
        block_on_async(async move {
            disp.dispatch(&req, move |_| {
                let sent = match block_on_async(async move { mailbox.send_message(&msg).await }) {
                    Ok(res) => Ok(res.count),
                    Err(e) => Err(e.to_string()),
                };
                mail_outcome(sent, key)
            })
            .await
        }),
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
}

/// Build + publish ONE kind-43001 for a coalesced mention burst, from SYNC
/// contexts (the mailbox runner). F8: publish on an isolated thread so we
/// never nest runtimes. Async contexts (the trigger loop) must instead build
/// via [`build_coalesced_job_event`] and `bus.publish(...).await` directly —
/// this function BLOCKS the calling thread on the publish.
pub fn publish_coalesced_job(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    coalesced: &Coalesced,
) -> Result<EventId, CoalescePublishError> {
    let event =
        build_coalesced_job_event(signer, coalesced).ok_or(CoalescePublishError::NoSigner)?;
    // F8: publish on an isolated thread so we never nest runtimes.
    // Bus is Sync; clone the event and publish via a scoped thread.
    let outcome = std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| format!("runner runtime: {e:#}"))?;
            rt.block_on(bus.publish(event.clone()))
                .map_err(|e| format!("bus publish: {e}"))
        })
        .join()
        .map_err(|_| "publish thread panicked".to_string())
    });
    match outcome {
        Ok(Ok(_)) => Ok(event.id),
        Ok(Err(e)) | Err(e) => Err(CoalescePublishError::Transport(e)),
    }
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
    workflows: Vec<Workflow>,
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
}

impl std::fmt::Debug for WorkflowHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHost")
            .field("workflows", &self.workflows.len())
            .field("engine", &self.engine)
            .field("has_job_signer", &self.job_signer.is_some())
            .field(
                "has_dispatcher",
                &self
                    .dispatcher
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
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
        let job_signer = mailbox.operator().cloned();
        // bd-cnr5 + bd-1kr2: production touches the durable journal — when
        // the mailbox carries the index pool, run states + terminal dedupe
        // persist in workflow_runs / workflow_terminal_events instead of
        // dying with the host process, and the effect arms dispatch
        // through the fenced EffectDispatcher (Spec 5.3).
        let store = mailbox
            .index_pool()
            .map(|pool| WorkflowEffectStore::new(pool.clone()));
        let dispatcher = Arc::new(std::sync::Mutex::new(
            store.clone().map(EffectDispatcher::new),
        ));
        let pending = match store {
            Some(store) => PendingDrain::new().with_store(store),
            None => PendingDrain::new(),
        };
        let runner = mailbox_runner_with_dispatcher(
            mailbox.clone(),
            coalescer.clone(),
            job_signer.clone(),
            dispatcher.clone(),
        );
        // bd-zczp: the engine prefers the context-bearing effect runner
        // for mailbox/job HostSink actions (Spec 1); the legacy compat
        // runner above stays for pure actions and journal-less fallbacks.
        let effect_runner = mailbox_effect_runner(
            mailbox.clone(),
            coalescer.clone(),
            job_signer.clone(),
            dispatcher.clone(),
        );
        // bd-engine-run-path-journaling-fwlx (the Spec 6 flip): the host
        // really dispatches, so its engine is Enforced — an effectful leaf
        // with no journal seam fails loud instead of running unjournaled.
        // bd-mi9n: the engine also reports its OWN run lifecycle through the
        // same store, so an engine-driven run's `workflow_runs` row exists
        // without the host calling `track_run_durable` — and both go through
        // the one `apply_run_status` path (no double-write).
        let run_journal = pending.store().map(|store| run_journal(store.clone()));
        let engine = match toron_workflow::engine::resonate_server_url() {
            Some(url) => WorkflowEngine::remote_with_resolvers_effect_mode_and_run_journal(
                url,
                runner,
                WorkflowEngine::default_resolver(),
                WorkflowEngine::default_workflow_resolver(),
                Some(effect_runner),
                EffectJournalMode::Enforced,
                run_journal,
            )
            .context("workflow engine (remote resonate server)")?,
            None => WorkflowEngine::local_with_resolvers_effect_mode_and_run_journal(
                runner,
                WorkflowEngine::default_resolver(),
                WorkflowEngine::default_workflow_resolver(),
                Some(effect_runner),
                EffectJournalMode::Enforced,
                run_journal,
            )
            .context("workflow engine")?,
        };
        let bus = mailbox.bus().clone();
        Ok(Self {
            engine: Arc::new(engine),
            bus,
            workflows,
            coalescer,
            job_signer,
            pending: Arc::new(tokio::sync::Mutex::new(pending)),
            dispatcher,
        })
    }

    /// Construct over an explicit bus (tests / embeddings without a mailbox).
    /// Call [`Self::with_job_signer`] to enable coalesced 43001 publishing.
    pub fn over_bus(
        bus: Arc<dyn Bus>,
        runner: ActionRunner,
        workflows: Vec<Workflow>,
    ) -> Result<Self> {
        let engine = WorkflowEngine::local(runner).context("workflow engine")?;
        Ok(Self {
            engine: Arc::new(engine),
            bus,
            workflows,
            coalescer: Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            job_signer: None,
            pending: Arc::new(tokio::sync::Mutex::new(PendingDrain::new())),
            dispatcher: Arc::new(std::sync::Mutex::new(None)),
        })
    }

    /// Construct over an explicit bus AND engine (bd-asx3 lifecycle tests:
    /// a remote Resonate engine is required for real promise parks).
    pub fn over_bus_with_engine(
        bus: Arc<dyn Bus>,
        engine: WorkflowEngine,
        workflows: Vec<Workflow>,
    ) -> Self {
        Self {
            engine: Arc::new(engine),
            bus,
            workflows,
            coalescer: Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            job_signer: None,
            pending: Arc::new(tokio::sync::Mutex::new(PendingDrain::new())),
            dispatcher: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Set the key that signs coalesced kind-43001 job requests (F7). Without
    /// a signer, flushes are logged but not published.
    pub fn with_job_signer(mut self, keys: Option<Keys>) -> Self {
        self.job_signer = keys;
        self
    }

    /// Attach the durable effect journal (bd-cnr5 + bd-1kr2): run states +
    /// terminal dedupe persist in `workflow_runs` / `workflow_terminal_events`,
    /// and the shared dispatcher cell gains an `EffectDispatcher`, so the
    /// runner arms fence sends without a rebuild. Production (`new`)
    /// attaches automatically when the mailbox carries the index pool;
    /// tests attach explicitly.
    pub async fn with_effect_store(&self, store: WorkflowEffectStore) {
        self.pending.lock().await.set_store(store.clone());
        *self.dispatcher.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(EffectDispatcher::new(store));
    }

    /// The fenced effect dispatcher (bd-1kr2, Spec 5.3): `Some` when the
    /// host was constructed over a journal (`new` with an index pool, or
    /// after [`Self::with_effect_store`]); `None` on journal-less
    /// embeddings, where the arms send directly.
    pub fn effect_dispatcher(&self) -> Option<EffectDispatcher> {
        self.dispatcher
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The engine (for tests / embedding observability).
    pub fn engine(&self) -> &WorkflowEngine {
        self.engine.as_ref()
    }

    /// The coalescer (for tests).
    pub fn coalescer(&self) -> &Arc<std::sync::Mutex<MentionCoalescer>> {
        &self.coalescer
    }

    /// Register an additional workflow (idempotent by name).
    pub fn add_workflow(&mut self, workflow: Workflow) {
        if !self.workflows.iter().any(|w| w.name == workflow.name) {
            self.workflows.push(workflow);
        }
    }

    /// The registered workflows.
    pub fn workflows(&self) -> &[Workflow] {
        &self.workflows
    }

    /// The host-owned pending lifecycle (bd-asx3).
    pub fn pending(&self) -> &Arc<tokio::sync::Mutex<PendingDrain>> {
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
        let body = serde_json::json!({
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
        // bd-cnr5: parse the status LOUD before any side effect —
        // unrecognized strings never silently become Failed.
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
        // bd-cnr5: durable dedupe is authoritative when a store is
        // attached; memory mirrors it. Duplicate short-circuits before
        // any bus write.
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
        let outcome = serde_json::json!({
            "status": status,
            "run_id": run_id,
            "failed_step": node,
            "evidence": evidence,
        });
        let event = toron_workflow::engine::build_workflow_run_terminal(signer, run_id, &outcome)
            .map_err(|e| anyhow::anyhow!("build 46012: {e}"))?;
        self.bus
            .publish(event)
            .await
            .map_err(|e| anyhow::anyhow!("publish 46012: {e}"))?;
        if has_store {
            // Journal the publication; a crash between bus publish and
            // this mark retries the same digest (idempotent, never a
            // second semantic terminal).
            if let Some(store) = self.pending.lock().await.store().cloned() {
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
                    .await?
            }
            ResolutionVerdict::Rejected => {
                self.engine
                    .reject_approval(&entry.promise_id, &Decision::rejected(res.note))
                    .await?
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
        // bd-cnr5: durable track — the workflow_runs row outlives the host.
        if let Err(e) = self
            .pending
            .lock()
            .await
            .track_run_durable(&run_id, workflow.clone())
            .await
        {
            warn!(run_id = %run_id, "durable run track failed (memory still tracked): {e:#}");
        }
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
                // Skip workflow-emitted rumors (loop prevention belt).
                let stamped = mail.rumor.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("t")
                        && t.as_slice().get(1).map(String::as_str)
                            == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
                });
                if stamped {
                    continue;
                }
                let kind = mail.rumor.kind.as_u16();
                for workflow in &me.workflows {
                    let Some(trigger) = &workflow.trigger else {
                        continue;
                    };
                    // MessagePosted matches kind 14/15 (DM) after unwrap.
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
                    // bd-asx3: detached run + drain, same contract as the bus loop.
                    me.spawn_run_and_drain(run_id, workflow.clone(), input)
                        .await;
                }
                me.drain_all_known().await;
            }
        })
    }

    /// Drive the trigger loop to completion (until the subscription closes).
    pub async fn run_trigger_loop(&self) -> Result<()> {
        // TRUSTED ingress (P11.1): a plain global `subscribe` would be gated
        // from every project-scoped event (A3: global subs never see `#t`
        // scoped events) — reservations (30031) and reactions (7) carry
        // project scoping, so the reactive ingress MUST bypass the gate.
        let mut sub = self
            .bus
            .subscribe_trusted(toron_core::bus::Filter::new())
            .await
            .map_err(|e| anyhow::anyhow!("bus subscribe_trusted: {e}"))?;
        while let Some(ev) = sub.next().await {
            let SubscriptionEvent::Event(event) = ev else {
                continue;
            };
            // ADR-0004 Ship 2: loop prevention at ingress — reject
            // workflow/job/gift-wrap emissions + the workflow-emitted tag.
            if !should_evaluate_trigger_event(&event) {
                debug!(kind = event.kind.as_u16(), "event filtered at ingress");
                continue;
            }
            debug!(kind = event.kind.as_u16(), "event passes ingress filter");
            // Tick the coalescer: flush any windowed @mention burst whose
            // window elapsed, and PUBLISH one kind-43001 per flushed batch
            // (ADR-0007 F7 — N mentions in a window → exactly one job).
            {
                // P1 (bd-p1-poison-on-panic-audit-jwpd): recover the std
                // Mutex poison — a panic inside the guard must not brick
                // every subsequent tick flush.
                // P1 (F3.COALESCER-LOCK, batch-fix): the guard is scoped to
                // tick() ONLY — the flushed Coalesced values are owned, the
                // guard drops, THEN each publish runs.
                let flushed = {
                    let mut c = self.coalescer.lock().unwrap_or_else(|e| e.into_inner());
                    c.tick(now_ms())
                };
                if !flushed.is_empty() {
                    debug!("coalescer flushed {} assignee(s)", flushed.len());
                }
                for coalesced in flushed {
                    // We are ON an async runtime here: build the event and
                    // await the publish directly. publish_coalesced_job's
                    // scoped-thread block_on is for the SYNC runner only —
                    // calling it here would block this executor thread for
                    // the full publish duration (bug hunt 2026-08-17).
                    match build_coalesced_job_event(self.job_signer.as_ref(), &coalesced) {
                        Some(event) => {
                            let id = event.id;
                            match self.bus.publish(event).await {
                                Ok(_) => debug!(
                                    assignee = %coalesced.assignee, %id,
                                    "kind-43001 published for coalesced burst"
                                ),
                                Err(e) => warn!(
                                    assignee = %coalesced.assignee, %id,
                                    "coalesced burst publish failed: {e}"
                                ),
                            }
                        }
                        None => warn!(
                            assignee = %coalesced.assignee,
                            "coalesced burst NOT published (no job signer configured)"
                        ),
                    }
                }
            }
            for workflow in &self.workflows {
                let Some(trigger) = &workflow.trigger else {
                    continue;
                };
                if !matches_trigger(trigger, &event) {
                    continue;
                }
                // ADR-0004 Ship 4: bounded guard evaluation at trigger gate.
                // Build the guard vars from the trigger payload the same way
                // the engine's branch-guard path does (`trigger_<field>`).
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
                // bd-asx3: detached run + immediate drain — a parked run
                // never holds this loop, and its parks are structured at once.
                self.spawn_run_and_drain(run_id, workflow.clone(), input)
                    .await;
            }
            // bd-asx3: re-drain every tracked run per tick, so parks created
            // after a spawn are picked up independently of any one await.
            self.drain_all_known().await;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Host-owned pending lifecycle (ADR-0032 Spec 6, bd-asx3).
//
// The engine parks on promises and pushes bare ids into its sinks; this host
// drains those ids into structured [`PendingEntry`] records, publishes the
// pending events (kind-46011 approval-pending, kind-43001 spawn-await job),
// validates resolutions, and publishes one semantic terminal (kind-46012)
// per digest. Runs execute detached from the ingress await so a parked run
// never holds the trigger loop, and a suspended run is tracked as
// [`RunState::Suspended`] — never logged as failed.
// ---------------------------------------------------------------------------

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
    fn is_park(action: &Action, kind: ParkKind) -> bool {
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
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Running | Self::Suspended | Self::ReconcilePending
        )
    }

    /// Canonical wire string for this state.
    pub fn as_str(self) -> &'static str {
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
    pub fn to_run_status(self) -> toron_core::index::workflow_effects::RunStatus {
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
    pub fn from_run_status(s: toron_core::index::workflow_effects::RunStatus) -> Self {
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
    pub fn store(&self) -> Option<&WorkflowEffectStore> {
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
        if let Some(state) = self.runs.get(run_id).copied() {
            // Memory wins only when it actually observed the run; a fresh
            // drain after restart has no entry and must read the journal.
            // `Running` is also the never-observed default, so fall through
            // to the store when memory holds just the default.
            if state != RunState::Running || self.store.is_none() {
                return state;
            }
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
        // Mirror into memory so the fast path converges.
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
        // Fresh sink ids exist: the run parked, even before attribution.
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
                node,
                None,
                id.clone(),
                *generation,
                ctx.kind,
            );
            self.known.insert(id);
            if self.registry.observe(entry.clone()) {
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
        // Look up by (run, promise) at the newest generation: a claimed
        // older generation must surface as StaleGeneration, not UnknownPromise.
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
    pub fn ignored_count(&self) -> usize {
        self.ignored.len()
    }
}

// ---------------------------------------------------------------------------
// Fenced effect dispatch (ADR-0032 Spec 5.3, bd-v139.2).
//
// [`EffectDispatcher`] executes the [`toron_workflow::engine`] reconciliation
// policy against [`WorkflowEffectStore`]: commit prepared intent before any
// provider call, keep one effect key across attempts, fence every attempt
// with a claim token, settle only with the current token, journal ambiguous
// outcomes as `unknown` (never inferred absence), and link mailbox `corr`
// by projection without touching the receipt store.
// ---------------------------------------------------------------------------

use toron_core::index::workflow_effects::{
    ClaimAttempt, EffectState, NewAttempt, NewEffect, NewRun, RunStatus, WorkflowEffect,
    WorkflowEffectStore, now_us,
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

impl EffectDispatcher {
    pub fn new(store: WorkflowEffectStore) -> Self {
        Self { store }
    }

    pub fn store(&self) -> &WorkflowEffectStore {
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
            return Ok(self.result_from_row(&row));
        }
        let now = now_us();
        // Live means an owner may be dispatching right now: a token pinned
        // by an unexpired lease. A token with no lease (classified outcome)
        // still fences at claim time but does not block planning.
        let lease_live = matches!(row.lease_expires_at, Some(expires) if expires > now);
        let view = EffectView {
            state: row.state,
            attempt: row.attempt,
            lease_live,
        };
        let attempt_no = match plan_attempt(Some(&view), &req.capability) {
            DispatchPlan::DispatchNew => row.attempt,
            DispatchPlan::RetrySameKey { attempt } => attempt,
            DispatchPlan::DuplicateTerminal => return Ok(self.result_from_row(&row)),
            DispatchPlan::ReconcileFirst { reason } | DispatchPlan::RefuseBlindRetry { reason } => {
                anyhow::bail!("dispatch refused for {}: {reason}", req.effect_key)
            }
        };
        // Fencing identity is deterministic per (key, attempt): unique
        // across attempts without a randomness dependency (the fence
        // defeats stale writers via the journal, not via token secrecy).
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
            anyhow::bail!(
                "dispatch fenced for {}: live owner holds the lease",
                req.effect_key
            );
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
        let outcome = provider(req);
        let (to, failure, value) = classify_outcome(&outcome);
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
                evidence: Some(match &outcome {
                    ProviderOutcome::Delivered(v) => v.clone(),
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
                    let view = EffectView {
                        state: r.state,
                        attempt: r.attempt,
                        lease_live,
                    };
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
        // A user kind-14 (no stamp) → evaluated.
        let user = kind14("hello", vec![]);
        assert!(
            should_evaluate_trigger_event(&user),
            "user message evaluates"
        );
        // The same kind-14 carrying the workflow-emitted stamp → rejected.
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
        // A schedule trigger is never event-driven.
        assert!(!matches_trigger(
            &Trigger::Schedule {
                cron: "* * * * *".into(),
                owner: None,
                scope: Default::default()
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

        // Recording runner: each dispatched action records the workflow name.
        let fired: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let fired2 = fired.clone();
        let runner: ActionRunner = Arc::new(move |a: &Action| {
            fired2
                .lock()
                .unwrap()
                .push(format!("action:{}", a.type_name()));
            Ok(serde_json::json!({ "action": a.type_name() }))
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
                    error_kinds: Vec::new(),
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
        let host = WorkflowHost::over_bus(bus.clone(), runner, workflows).expect("host");
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

        // 1. A plain acquire (status-less content) → on-acquire only.
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
        // Disambiguation: the acquire must NOT have fired on-release.
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

        // 2. A release marker (status:released content) → on-release fires,
        //    and does NOT re-fire on-acquire.
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

        // 3. A kind-7 reaction → reaction_added fires.
        fired.lock().unwrap().clear();
        let react = EventBuilder::new(Kind::from_u16(7), "+")
            .finalize(&keys)
            .expect("sign");
        bus.publish(react).await.expect("publish reaction");
        assert!(
            wait_for(&fired, "action:send_message", Duration::from_secs(10)).await,
            "reaction fired the reaction_added workflow: {fired:?}"
        );

        // 4. Loop prevention end-to-end: a WORKFLOW-EMITTED (stamped) kind-14
        //    does NOT fire message_posted; a user kind-14 does.
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
        let runner = mailbox_runner(mailbox, coalescer.clone(), None);
        let action = |i: u32| Action::DispatchJob {
            assignee: "oc-beta".into(),
            spec: crate::workflow_host::test_spec(json!({ "i": i })),
            channel: None,
        };
        for i in 0..5 {
            let out = runner(&action(i)).expect("runner ok");
            assert!(out["ok"].as_bool() == Some(true), "dispatch {i} ok");
        }
        // All five @mentions are buffered in the coalescer (window open),
        // NOT emitted as five separate jobs — one buffered batch.
        let c = coalescer.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            c.buffered_for("oc-beta") == 5,
            "all mentions buffered for oc-beta, got {}",
            c.buffered_for("oc-beta")
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

        // The workflow sender + a recipient in project "demo" (sender_for →
        // "workflow-demo").
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
        let runner = mailbox_runner(mailbox.clone(), coalescer, None);
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");

        // The runner is SYNC and block_ons the mailbox (F8 — known); drive it
        // on plain OS threads that enter their own runtime.
        let rt = Arc::new(tokio::runtime::Runtime::new().expect("runner runtime"));
        let send = |action: Action| {
            let runner = runner.clone();
            let rt = rt.clone();
            let out = std::thread::spawn(move || {
                let _guard = rt.enter();
                runner(&action).expect("runner ok")
            })
            .join()
            .expect("runner thread");
            assert!(out["ok"].as_bool() == Some(true), "runner ok: {out}");
        };
        send(Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "review".into(),
            body: "stamped body".into(),
            project: Some("demo".into()),
        });
        send(Action::SendDm {
            to: "BlueLake".into(),
            body: "stamped dm".into(),
            project: Some("demo".into()),
        });

        // Two wraps arrive (one per send); both rumors must carry the stamp.
        for _ in 0..2 {
            let wrap = loop {
                match tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await {
                    Ok(Some(SubscriptionEvent::Event(e))) => break e,
                    Ok(Some(SubscriptionEvent::Eose)) | Ok(None) => continue,
                    Ok(Some(SubscriptionEvent::Closed(_))) => panic!("subscription closed"),
                    Err(_) => panic!("timed out waiting for wrap event"),
                }
            };
            // The wrap envelope stays metadata-hiding: no stamp on kind-1059.
            let wrap_has_stamp = wrap.tags.iter().any(|t| {
                t.as_slice().first().map(String::as_str) == Some("t")
                    && t.as_slice().get(1).map(String::as_str)
                        == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
            });
            assert!(!wrap_has_stamp, "gift wrap must not carry the stamp");
            // The rumor inside carries it.
            let unsigned = unwrap_for_recipient(&recv_keys, &wrap)
                .expect("unwrap")
                .rumor;
            let has_stamp = unsigned.tags.iter().any(|t| {
                t.as_slice().first().map(String::as_str) == Some("t")
                    && t.as_slice().get(1).map(String::as_str)
                        == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
            });
            assert!(has_stamp, "workflow-originated rumor carries the stamp");
            // 0.45 UnsignedEvent::finalize signs the rumor's existing pubkey;
            // the recipient key cannot finalize a sender rumor. Rebuild so
            // the filter sees the same kind + stamp tags.
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
        // A runtime cannot be dropped inside an async context — drop it on a
        // plain thread (shutdown does blocking work).
        std::thread::spawn(move || drop(rt))
            .join()
            .expect("drop rt");
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

        // Cap-3 coalescer: the third DispatchJob mention flushes NOW through
        // the runner (the host's tick never sees it).
        let signer = Keys::generate();
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
            std::time::Duration::from_secs(2),
            std::time::Duration::from_millis(500),
            3,
        )));
        let runner = mailbox_runner(mailbox, coalescer.clone(), Some(signer.clone()));
        // Global sub: assignee is NOT a project slug (`assignee` tag, not `#t`).
        // Stamped `t=toron:workflow-emitted` is ignored by access_gate.
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");
        let mut published = 0;
        for i in 0..3 {
            let runner = runner.clone();
            let out = std::thread::spawn(move || {
                runner(&Action::DispatchJob {
                    assignee: "oc".into(),
                    spec: crate::workflow_host::test_spec(json!({ "i": i })),
                    channel: None,
                })
                .expect("runner ok")
            })
            .join()
            .expect("runner thread");
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
        let bus_for_thread = bus.clone();
        let signer_for_thread = signer.clone();
        let id = std::thread::spawn(move || {
            publish_coalesced_job(bus_for_thread.as_ref(), Some(&signer_for_thread), &batch)
                .expect("host publish")
        })
        .join()
        .expect("publish thread");
        assert!(!id.to_hex().is_empty());

        let mut jobs43001 = Vec::new();
        for _ in 0..2 {
            let ev = loop {
                match tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await {
                    Ok(Some(SubscriptionEvent::Event(e))) if e.kind.as_u16() == 43001 => break e,
                    Ok(Some(SubscriptionEvent::Event(_))) => continue,
                    Ok(Some(SubscriptionEvent::Eose)) | Ok(None) => continue,
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

        // Default window semantics (2s window, 500ms sub-window, cap 64).
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let runner = mailbox_runner(mailbox, coalescer.clone(), Some(Keys::generate()));

        // Fire 100 DispatchJob mentions from 8 threads (barrier-synced).
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let mut handles = Vec::new();
        for t in 0..8 {
            let runner = runner.clone();
            let barrier = barrier.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for i in 0..(MENTIONS / 8) {
                    runner(&Action::DispatchJob {
                        assignee: "oc".into(),
                        spec: crate::workflow_host::test_spec(json!({ "i": t * 1000 + i })),
                        channel: None,
                    })
                    .expect("runner ok");
                }
            }));
        }
        for h in handles {
            h.join().expect("burst thread");
        }

        // Count the published 43001s (cap-flush path publishes inline; the
        // window flushes are exercised by the coalescer itself in unit tests).
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

        // Recording runner: counts how many times each action fires.
        let ran = Arc::new(AtomicUsize::new(0));
        let ran2 = ran.clone();
        let runner: ActionRunner = Arc::new(move |a: &Action| {
            if let Action::SendDm { .. } = a {
                ran2.fetch_add(1, Ordering::Relaxed);
            }
            Ok(serde_json::Value::Null)
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

        let host = WorkflowHost::over_bus(bus.clone(), runner, vec![wf]).expect("host");
        let handle = host.spawn_trigger_loop();

        // Publish a matching kind-14 event → the loop must invoke the workflow.
        let ev = kind14("triage me", vec![]);
        bus.publish(ev).await.expect("publish");

        // Let the loop process + run.
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
    /// sends must not hang or panic. The runner is sync; every mailbox call
    /// goes through `block_on_async` (dedicated OS thread + its OWN
    /// current-thread runtime). Driving N runners concurrently from plain
    /// threads (no ambient runtime) proves no nested `block_on` on a shared
    /// Tokio handle: each call runs on an isolated runtime.
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
        let runner = mailbox_runner(mailbox.clone(), coalescer, None);

        // All N threads fire at once (barrier) — maximal contention through
        // the sync runner → block_on_async → mailbox gift-wrap + publish.
        let barrier = Arc::new(std::sync::Barrier::new(CONCURRENCY));
        let handles: Vec<_> = (0..CONCURRENCY)
            .map(|i| {
                let runner = runner.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    runner(&Action::SendMessage {
                        to: vec!["BlueLake".into()],
                        subject: format!("stress {i}"),
                        body: format!("stress body {i}"),
                        project: Some("demo".into()),
                    })
                })
            })
            .collect();
        let mut ok = 0usize;
        for h in handles {
            let out = h
                .join()
                .expect("runner thread must not panic (no nested runtime)");
            let delivered = out
                .as_ref()
                .ok()
                .and_then(|v| v.get("ok").and_then(|b| b.as_bool()))
                == Some(true);
            assert!(delivered, "concurrent send must deliver: {out:?}");
            ok += 1;
        }
        assert_eq!(ok, CONCURRENCY, "all {CONCURRENCY} sends delivered");

        // Every send landed on the bus as a stamped gift wrap.
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
    /// std-Mutex guard must NOT be held across `publish_coalesced_job`'s
    /// blocking publish (thread::scope + rt.block_on).
    ///
    /// GREEN since the batch fix landed: at BOTH publish sites the coalesced
    /// job is cloned under the lock, the guard drops, THEN the publish runs
    /// (runner FlushNow arm + trigger-loop tick flush), so a concurrent flush
    /// for an unrelated assignee proceeds immediately. The trigger loop now
    /// publishes by direct `bus.publish(...).await` (no scoped-thread
    /// block_on on the executor thread); the runner keeps the sync path.
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

        // cap=3 → the THIRD mention to an assignee flushes NOW (publish path).
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
            std::time::Duration::from_millis(2_000),
            std::time::Duration::from_millis(500),
            3,
        )));
        let runner = mailbox_runner(mailbox.clone(), coalescer.clone(), Some(Keys::generate()));

        let dispatch = |runner: &ActionRunner, assignee: &str, text: &str| {
            runner(&Action::DispatchJob {
                assignee: assignee.into(),
                spec: crate::workflow_host::test_spec(json!({ "text": text })),
                channel: None,
            })
        };

        // Prime oc's buffer (two mentions buffered, no flush).
        let _ = dispatch(&runner, "oc", "e1");
        let _ = dispatch(&runner, "oc", "e2");

        // A: third oc mention → cap flush → publish_coalesced_job BLOCKS on
        // the slow bus (while the coalescer guard is held today).
        let runner_a = runner.clone();
        let a = std::thread::spawn(move || dispatch(&runner_a, "oc", "e3"));

        // B: an unrelated flush must NOT wait for A's slow publish.
        std::thread::sleep(std::time::Duration::from_millis(100));
        let runner_b = runner.clone();
        let t0 = std::time::Instant::now();
        let b = std::thread::spawn(move || dispatch(&runner_b, "gk", "e4"));
        let _ = b.join().expect("runner B no panic");
        let b_elapsed = t0.elapsed();
        let _ = a.join().expect("runner A no panic");

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
    #[test]
    fn poisoned_coalescer_survives() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-poison-{}.sqlite", uuid::Uuid::new_v4()));
        let (runner, coalescer) = tokio::runtime::Runtime::new().expect("rt").block_on(async {
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
            let runner = mailbox_runner(mailbox.clone(), coalescer.clone(), Some(Keys::generate()));
            (runner, coalescer)
        });

        // Poison the coalescer: panic while holding the guard (std Mutex
        // poisoning semantics — the guard's Drop during unwind poisons).
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

        // Survivability: a subsequent flush through the REAL runner arm must
        // not panic (recover the poison, keep serving).
        let runner = runner.clone();
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            runner(&Action::DispatchJob {
                assignee: "oc".into(),
                spec: crate::workflow_host::test_spec(json!({ "text": "after-poison" })),
                channel: None,
            })
        }));
        assert!(
            out.is_ok(),
            "poisoned coalescer must survive — subsequent flush panicked: {out:?}"
        );
        let _ = std::fs::remove_file(&db);
    }

    /// bd-i6kc RED: build a production runner over a throwaway mailbox.
    async fn honesty_mailbox_runner() -> ActionRunner {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-honesty-{}.sqlite", uuid::Uuid::new_v4()));
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
            std::env::temp_dir().join(format!("wf-honesty-data-{}", uuid::Uuid::new_v4())),
            None,
        ));
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        mailbox_runner(mailbox, coalescer, None)
    }

    /// bd-asx3 RED: draining a newly parked promise records one structured
    /// entry (run/node/promise/generation) and suspends the run; a second
    /// drain observes no duplicates.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_lifecycle_drain_records_structured_entry() {
        use toron_workflow::engine::ParkKind;
        let engine = WorkflowEngine::local(Arc::new(|_: &Action| Ok(serde_json::Value::Null)))
            .expect("local engine");
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

    /// bd-asx3 RED: restart recovery — a fresh drain over the same live
    /// sinks reconstructs the identical registry without an in-memory carry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn host_lifecycle_restart_recovery() {
        use toron_workflow::engine::ParkKind;
        let engine = WorkflowEngine::local(Arc::new(|_: &Action| Ok(serde_json::Value::Null)))
            .expect("local engine");
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
        // Restart: brand-new drain, same live sinks.
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
        let engine = WorkflowEngine::local(Arc::new(|_: &Action| Ok(serde_json::Value::Null)))
            .expect("local engine");
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
        let runner: ActionRunner = Arc::new(|_: &Action| Ok(serde_json::Value::Null));
        let host = WorkflowHost::over_bus(bus.clone(), runner, vec![])
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

        // Terminal: same digest twice → one semantic terminal on the bus.
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

    /// bd-asx3 e2e: a real parked run is SUSPENDED (never failed), drains
    /// into the registry independently of the run await, and resumes to
    /// Succeeded through the validated host resolution path.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn host_lifecycle_parked_run_suspends_and_resolves() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        use toron_workflow::engine::ResolutionVerdict;

        // Real `resonate serve` (same requirement as the engine WF-08 tests).
        let dir = std::env::temp_dir().join(format!("wf-life-rsrv-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("tmpdir");
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .expect("port")
            .local_addr()
            .expect("addr")
            .port();
        let url = format!("http://127.0.0.1:{port}");
        let mut server = std::process::Command::new("resonate")
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
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("resonate serve");
        // Wait for the server port (up to ~10s).
        let mut ready = false;
        for _ in 0..100 {
            if std::net::TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
                ready = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        if !ready {
            let _ = server.kill();
            let _ = server.wait();
            eprintln!("SKIP: resonate serve did not come up");
            return;
        }

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
        let runner: ActionRunner = Arc::new(|_: &Action| Ok(serde_json::json!({"ok": true})));
        let engine = WorkflowEngine::remote(&url, runner).expect("remote engine");
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

        // The run parks: Suspended, structured, and NOT failed.
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

        // Operator approves through the validated host path.
        let res = toron_workflow::engine::Resolution {
            run_id: "run-e2e-1".into(),
            project: None,
            promise_id: entries[0].promise_id.clone(),
            generation: 1,
            verdict: ResolutionVerdict::Approved,
            note: "ship it".into(),
        };
        host.resolve_parked(res).await.expect("resolve parked");

        // The parked run resumes and completes.
        let mut done = false;
        for _ in 0..150 {
            if host.pending().lock().await.run_state("run-e2e-1") == RunState::Succeeded {
                done = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(done, "run resumes to Succeeded after validated resolution");
        let _ = server.kill();
        let _ = server.wait();
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
            .dispatch(&recon_req("ek-1"), |req| {
                let row = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(store.get_effect(&req.effect_key))
                })
                .expect("read")
                .expect("intent committed before dispatch");
                seen2
                    .lock()
                    .unwrap()
                    .push((row.effect_key, row.state.as_str().to_string()));
                ProviderOutcome::Delivered(serde_json::json!({"ok": true}))
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
        // New owner rotates the claim (expired-lease recovery path).
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
        // Stale owner settles with the old token: refused.
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
        // Fixed cleanup (lease release by bead reason) journals separately.
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
        // Simulate a crash between claim and classification: prepared intent
        // plus a live-looking claim with an already-expired lease.
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
        // Settled + expired retention: prunable, with its attempts.
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
        // Unknown + expired retention: uncertainty survives.
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
        // Settled + live retention: survives.
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
        // In-flight: survives regardless.
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
        // Pending: never auto-pruned.
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
        // A fully quiesced run (terminal, no live effects) IS prunable.
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
        let runner = honesty_mailbox_runner().await;
        let out = runner(&Action::React {
            message_id: "deadbeef".into(),
            emoji: "+1".into(),
        })
        .expect("runner returns a value, never Err");
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
        let runner = honesty_mailbox_runner().await;
        let out = runner(&Action::CallWebhook {
            url: "https://example.com/hook".into(),
            method: None,
            body: None,
        })
        .expect("runner returns a value, never Err");
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
            error_kinds: vec![],
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
        let runner = honesty_mailbox_runner().await;
        for (name, action) in &cases {
            let out = runner(action).expect("runner returns a value, never Err");
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
        // Restart: brand-new drain over the SAME store recovers the state.
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
    #[test]
    fn run_ledger_flushnow_unpublished_is_failure() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;
        let rt = tokio::runtime::Runtime::new().expect("rt");
        rt.block_on(async {
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
            // Cap-1 coalescer: the FIRST mention flushes NOW with no signer.
            let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
                std::time::Duration::from_secs(60),
                std::time::Duration::from_secs(60),
                1,
            )));
            let runner = mailbox_runner(mailbox, coalescer, None);
            let out = runner(&Action::DispatchJob {
                assignee: "oc".into(),
                spec: crate::workflow_host::test_spec(serde_json::json!({ "t": 1 })),
                channel: None,
            })
            .expect("runner ok");
            assert_eq!(
                out["ok"],
                serde_json::json!(false),
                "unpublished flush must report ok:false, got {out}"
            );
            let _ = std::fs::remove_file(&db);
        });
    }

    /// bd-1kr2 RED: compat effect keys are stable — the same action mints
    /// the same key (idempotent retry attaches), a different payload mints
    /// a different key.
    #[test]
    fn prod_effect_fencing_compat_key_stable() {
        let a = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "review".into(),
            body: "stamped body".into(),
            project: Some("demo".into()),
        };
        let b = a.clone();
        assert_eq!(compat_effect_key(&a), compat_effect_key(&b));
        let c = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "review".into(),
            body: "different body".into(),
            project: Some("demo".into()),
        };
        assert_ne!(compat_effect_key(&a).0, compat_effect_key(&c).0);
    }

    /// bd-1kr2 RED helper: a dispatcher-backed production runner over
    /// throwaway sqlite, with sender + recipient registered in `demo`.
    async fn fencing_runner(
        tag: &str,
    ) -> (
        ActionRunner,
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
        let runner = mailbox_runner_with_dispatcher(
            mailbox,
            coalescer,
            None,
            Arc::new(std::sync::Mutex::new(Some(dispatcher.clone()))),
        );
        (runner, dispatcher, bus, db, data_dir, recv_keys)
    }

    /// Drive a sync runner from a plain OS thread (the runner block_ons
    /// internally; never call it from inside an async context directly).
    fn drive_runner(runner: &ActionRunner, action: Action) -> serde_json::Value {
        let runner = runner.clone();
        std::thread::spawn(move || runner(&action).expect("runner ok"))
            .join()
            .expect("runner thread")
    }

    /// bd-1kr2 RED: the REAL SendMessage arm journals intent before
    /// dispatch — a same-key retry attaches (one gift wrap on the wire,
    /// never two) and the journal projects corr == effect_key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prod_effect_fencing_send_message_sends_once_and_projects_corr() {
        use toron_core::mailbox::messaging::unwrap_for_recipient;
        let (runner, dispatcher, bus, db, data_dir, recv_keys) = fencing_runner("once").await;
        let action = Action::SendMessage {
            to: vec!["BlueLake".into()],
            subject: "fenced".into(),
            body: "exactly once".into(),
            project: Some("demo".into()),
        };
        let (key, _) = compat_effect_key(&action);
        let out1 = drive_runner(&runner, action.clone());
        assert_eq!(out1["ok"], serde_json::json!(true), "first send ok: {out1}");
        let out2 = drive_runner(&runner, action.clone());
        assert_eq!(
            out2["ok"],
            serde_json::json!(true),
            "same-key retry attaches: {out2}"
        );
        // Exactly one gift wrap on the wire — the retry dispatched nothing.
        let replay = toron_core::mailbox::collect_replay(
            bus.as_ref(),
            toron_core::bus::Filter::new().kinds([nostr::prelude::Kind::GiftWrap]),
        )
        .await
        .expect("replay");
        assert_eq!(replay.len(), 1, "one wrap for two same-key sends");
        // The rumor projects corr == effect_key (Spec 5.4).
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
        // The journal agrees: settled with transport_corr == key.
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
            let runner = mailbox_runner_with_dispatcher(
                mailbox,
                Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
                None,
                Arc::new(std::sync::Mutex::new(Some(EffectDispatcher::new(store)))),
            );
            let out = drive_runner(&runner, action.clone());
            assert_eq!(out["ok"], serde_json::json!(true));
            drop(runner);
        }
        // "Restart": brand-new mailbox + runner over the SAME pool/bus.
        {
            let mailbox = Arc::new(
                Mailbox::new(bus.clone(), data_dir.clone(), None).with_agent_passphrase(PASSPHRASE),
            );
            let store = toron_core::index::workflow_effects::WorkflowEffectStore::new(pool.clone());
            let runner = mailbox_runner_with_dispatcher(
                mailbox,
                Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
                None,
                Arc::new(std::sync::Mutex::new(Some(EffectDispatcher::new(store)))),
            );
            let out = drive_runner(&runner, action.clone());
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
        // Cap-1: every mention flushes NOW, with no signer to publish it.
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::new(
            std::time::Duration::from_secs(60),
            std::time::Duration::from_secs(60),
            1,
        )));
        let runner = mailbox_runner_with_dispatcher(
            mailbox,
            coalescer,
            None,
            Arc::new(std::sync::Mutex::new(Some(dispatcher.clone()))),
        );
        let action = Action::DispatchJob {
            assignee: "oc".into(),
            spec: crate::workflow_host::test_spec(serde_json::json!({ "nosign": 1 })),
            channel: None,
        };
        let (key, _) = compat_effect_key(&action);
        for _ in 0..2 {
            let out = drive_runner(&runner, action.clone());
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
        let (key, digest) = compat_effect_key(&action);
        let req = DispatchRequest {
            effect_key: key.clone(),
            run_id: "compat".into(),
            project: Some("demo".into()),
            workflow_digest: "compat".into(),
            node_path: action.type_name().into(),
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
        let runner = mailbox_effect_runner(
            mailbox,
            Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            None,
            cell,
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
        let call = |runner: &EffectRunner, invocation: ActionInvocation| {
            let runner = runner.clone();
            std::thread::spawn(move || runner(&invocation).expect("effect ok"))
                .join()
                .expect("effect thread")
        };
        let out_a1 = call(&runner, inv("run-A", "gate"));
        assert_eq!(out_a1["ok"], serde_json::json!(true));
        // Same logical invocation retries attach (one wire send).
        let out_a2 = call(&runner, inv("run-A", "gate"));
        assert_eq!(out_a2["ok"], serde_json::json!(true));
        // Identical payload, different run: a DIFFERENT effect (rescoped).
        let out_b = call(&runner, inv("run-B", "gate"));
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
        // The journal holds two run-scoped rows, keyed by
        // derive_effect_key — not the compat content hash.
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
        let runner = mailbox_effect_runner(
            mailbox,
            Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env())),
            None,
            cell,
        );
        let action = Action::Verify {
            command: "cargo test".into(),
        };
        // A distinct run per observation: the effect key is run-scoped, so
        // reusing one run would attach to the first row instead of dispatching.
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
            let runner = runner.clone();
            std::thread::spawn(move || runner(&i).expect("effect ok"))
                .join()
                .expect("effect thread")
        };

        let green = call(invocation(
            "run-verify-green",
            Some(serde_json::json!({ "ok": true, "exit": 0, "stdout_path": "r.txt" })),
        ));
        assert_eq!(
            green["ok"],
            serde_json::json!(true),
            "a green verify passes through: {green}"
        );
        let red = call(invocation(
            "run-verify-red",
            Some(serde_json::json!({ "ok": false, "exit": 101, "error": "tests failed" })),
        ));
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
        // The host attaches no `VerifySink` of its own, so the engine hands
        // over the sink's own missing-sink projection: name the real cause,
        // and journal it (an unjournaled verify is the bypass this closes).
        let missing = call(invocation("run-verify-missing", None));
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
}
