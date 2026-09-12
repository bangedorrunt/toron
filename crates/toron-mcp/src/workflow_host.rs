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
    ActionRunner, Decision, ParkKind, PendingEntry, PendingRegistry, Resolution,
    ResolutionMismatch, ResolutionVerdict, RunInput, TerminalLedger, TerminalNote, TerminalRecord,
    WorkflowEngine, attribute_parks, terminal_digest, validate_resolution,
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
                    // ADR-0007 F6: every workflow-originated rumor carries the
                    // `toron:workflow-emitted` stamp (gift wrap stays
                    // metadata-hiding — the stamp is INSIDE the rumor).
                    extra_tags: stamp_workflow_emitted(vec![]),
                };
                let mb = mailbox.clone();
                Some(
                    match block_on_async(async move { mb.send_message(&msg).await }) {
                        Ok(res) => Ok(serde_json::json!({
                            "ok": true,
                            "count": res.count,
                        })),
                        Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
                    },
                )
            }
            Action::SendDm { to, body, project } => {
                let project = project.clone().unwrap_or_default();
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
                    // ADR-0007 F6: workflow-originated DM rumors are stamped
                    // too — a workflow can never re-trigger itself.
                    extra_tags: stamp_workflow_emitted(vec![]),
                };
                let mb = mailbox.clone();
                Some(
                    match block_on_async(async move { mb.send_message(&msg).await }) {
                        Ok(res) => Ok(serde_json::json!({
                            "ok": true,
                            "count": res.count,
                        })),
                        Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
                    },
                )
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
                Some(match decision {
                    // Hard-cap flush: publish the coalesced 43001 NOW (the
                    // host's tick cannot — the buffer is already drained).
                    CoalesceDecision::FlushNow(coalesced) => {
                        let published = publish_coalesced_job(
                            mailbox.bus().as_ref(),
                            job_signer.as_ref(),
                            &coalesced,
                        );
                        Ok(serde_json::json!({
                            "ok": true,
                            "coalesced": false,
                            "published": published.is_some(),
                            "error": if published.is_some() { serde_json::Value::Null } else {
                                serde_json::json!("no job signer configured")
                            },
                        }))
                    }
                    CoalesceDecision::Buffered { .. } => Ok(serde_json::json!({
                        "ok": true,
                        "coalesced": true,
                    })),
                })
            }
            // Every other HostSink action (today: `Verify`) has no sink on
            // this mailbox-only runner — the boundary reports `missing_sink`.
            _ => None,
        });
        Ok(value)
    })
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

/// Build + publish ONE kind-43001 for a coalesced mention burst, from SYNC
/// contexts (the mailbox runner). F8: publish on an isolated thread so we
/// never nest runtimes. Returns the published event id, or `None` when no
/// signer is available. Async contexts (the trigger loop) must instead build
/// via [`build_coalesced_job_event`] and `bus.publish(...).await` directly —
/// this function BLOCKS the calling thread on the publish.
pub fn publish_coalesced_job(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    coalesced: &Coalesced,
) -> Option<EventId> {
    let event = build_coalesced_job_event(signer, coalesced)?;
    // F8: publish on an isolated thread so we never nest runtimes.
    // Bus is Sync; clone the event and publish via a scoped thread.
    let published = std::thread::scope(|s| {
        s.spawn(|| {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?;
            rt.block_on(bus.publish(event.clone())).ok()
        })
        .join()
        .ok()
        .flatten()
    });
    published?;
    Some(event.id)
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
}

impl std::fmt::Debug for WorkflowHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHost")
            .field("workflows", &self.workflows.len())
            .field("engine", &self.engine)
            .field("has_job_signer", &self.job_signer.is_some())
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
        let runner = mailbox_runner(mailbox.clone(), coalescer.clone(), job_signer.clone());
        let engine = match toron_workflow::engine::resonate_server_url() {
            Some(url) => WorkflowEngine::remote(url, runner)
                .context("workflow engine (remote resonate server)")?,
            None => WorkflowEngine::local(runner).context("workflow engine")?,
        };
        let bus = mailbox.bus().clone();
        Ok(Self {
            engine: Arc::new(engine),
            bus,
            workflows,
            coalescer,
            job_signer,
            pending: Arc::new(tokio::sync::Mutex::new(PendingDrain::new())),
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
        }
    }

    /// Set the key that signs coalesced kind-43001 job requests (F7). Without
    /// a signer, flushes are logged but not published.
    pub fn with_job_signer(mut self, keys: Option<Keys>) -> Self {
        self.job_signer = keys;
        self
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
        let digest = terminal_digest(run_id, workflow_digest, status, node, evidence);
        let record = TerminalRecord {
            run_id: run_id.to_string(),
            digest: digest.clone(),
            status: status.to_string(),
            node: node.to_string(),
        };
        let note = self.pending.lock().await.note_terminal(record);
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
        let terminal_state = match status {
            "succeeded" => RunState::Succeeded,
            "blocked" => RunState::Blocked,
            "cancelled" => RunState::Cancelled,
            "unknown" => RunState::Unknown,
            _ => RunState::Failed,
        };
        self.pending
            .lock()
            .await
            .set_run_state(run_id, terminal_state);
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
        self.pending
            .lock()
            .await
            .set_run_state(&entry.run_id, RunState::Running);
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
        self.pending
            .lock()
            .await
            .track_run(&run_id, workflow.clone());
        let me = self.clone();
        let rid = run_id.clone();
        tokio::spawn(async move {
            match me.engine.run(&rid, input).await {
                Ok(_) => {
                    me.pending
                        .lock()
                        .await
                        .set_run_state(&rid, RunState::Succeeded);
                    debug!(run_id = rid, "workflow ran");
                }
                Err(e) => {
                    me.pending
                        .lock()
                        .await
                        .set_run_state(&rid, RunState::Failed);
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
    Succeeded,
    Failed,
    Blocked,
    Cancelled,
    Unknown,
}

impl RunState {
    /// True once the run reached any terminal state.
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Running | Self::Suspended)
    }
}

/// Host-owned pending drain: registry + terminal ledger + per-(run, node)
/// generation counters + known promise ids + run states + recorded
/// mismatches. One drain serves every tracked run, independently of any
/// single `engine.run` await; re-draining the same live sinks after a
/// restart reconstructs the identical registry (deterministically
/// replayable — no durable row required at Wave 1).
#[derive(Debug, Default)]
pub struct PendingDrain {
    registry: PendingRegistry,
    ledger: TerminalLedger,
    generations: std::collections::HashMap<(String, String), u64>,
    known: std::collections::HashSet<String>,
    runs: std::collections::HashMap<String, RunState>,
    run_workflows: std::collections::HashMap<String, Workflow>,
    ignored: Vec<(Resolution, ResolutionMismatch)>,
}

impl PendingDrain {
    /// Empty drain.
    pub fn new() -> Self {
        Self::default()
    }

    /// Track a run's workflow for later re-drains (called when the run starts).
    pub fn track_run(&mut self, run_id: &str, workflow: Workflow) {
        self.run_workflows.insert(run_id.to_string(), workflow);
        self.runs.insert(run_id.to_string(), RunState::Running);
    }

    /// Current tracked state (`Running` when never observed).
    pub fn run_state(&self, run_id: &str) -> RunState {
        self.runs.get(run_id).copied().unwrap_or(RunState::Running)
    }

    /// Set the tracked state.
    pub fn set_run_state(&mut self, run_id: &str, state: RunState) {
        self.runs.insert(run_id.to_string(), state);
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
}
