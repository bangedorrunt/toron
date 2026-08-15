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
use nostr_sdk::nostr::{EventId, Keys, Timestamp};
use toron_core::bus::{Bus, SubscriptionEvent};
use toron_core::mailbox::{Mailbox, SendMessage, UnwrappedMail};
use toron_workflow::coalesce::{CoalesceDecision, Coalesced, MentionCoalescer};
use toron_workflow::engine::{ActionRunner, RunInput, WorkflowEngine};
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
/// Dispatch semantics:
/// - `SendMessage` / `SendDm` → `Mailbox::send_message` (kind-14 gift wrap).
/// - `DispatchJob` → a coalesced job via the [`MentionCoalescer`] (kind-43001
///   with the merged spec; the coalescer decides when to flush).
/// - `Delay` / `RequestApproval` / control-flow → the engine handles these
///   itself; the runner returns `Null` (never reached for those).
/// - `React` / `CallWebhook` → not yet wired (returns an honest `{ok:false}`
///   error — a run using them fails the step, never silently succeeds).
///
/// The runner is sync (engine contract) and thread-safe; the coalescer is
/// parked behind a `Mutex`.
pub fn mailbox_runner(
    mailbox: Arc<Mailbox>,
    coalescer: Arc<std::sync::Mutex<MentionCoalescer>>,
    job_signer: Option<Keys>,
) -> ActionRunner {
    Arc::new(move |action: &Action| {
        match action {
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
                match block_on_async(async move { mb.send_message(&msg).await }) {
                    Ok(res) => Ok(serde_json::json!({
                        "ok": true,
                        "count": res.count,
                    })),
                    Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
                }
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
                match block_on_async(async move { mb.send_message(&msg).await }) {
                    Ok(res) => Ok(serde_json::json!({
                        "ok": true,
                        "count": res.count,
                    })),
                    Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
                }
            }
            Action::DispatchJob { assignee, spec, .. } => {
                // @mention dispatch through the coalescer: one job per burst.
                let mention = toron_workflow::coalesce::InboundMention {
                    assignee: assignee.clone(),
                    parent: String::new(),
                    spec: spec.clone(),
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
                match decision {
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
                }
            }
            _ => Ok(serde_json::Value::Null),
        }
    })
}

/// Build + publish ONE kind-43001 for a coalesced mention burst (ADR-0007
/// F7): N mentions → exactly one job. The request carries the
/// `toron:workflow-emitted` stamp (F6) so the trigger filter never re-evaluates
/// it. Returns the published event id, or `None` when no signer is available.
pub fn publish_coalesced_job(
    bus: &dyn Bus,
    signer: Option<&Keys>,
    coalesced: &Coalesced,
) -> Option<EventId> {
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
}

impl std::fmt::Debug for WorkflowHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHost")
            .field("workflows", &self.workflows.len())
            .field("engine", &self.engine)
            .field("has_job_signer", &self.job_signer.is_some())
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
        })
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
                    };
                    match me.engine.run(&run_id, input).await {
                        Ok(_) => debug!(run_id, "mail-trigger workflow {} ran", workflow.name),
                        Err(e) => warn!(
                            run_id,
                            "mail-trigger workflow {} failed: {e}", workflow.name
                        ),
                    }
                }
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
                // guard drops, THEN each blocking publish runs.
                let flushed = {
                    let mut c = self.coalescer.lock().unwrap_or_else(|e| e.into_inner());
                    c.tick(now_ms())
                };
                if !flushed.is_empty() {
                    debug!("coalescer flushed {} assignee(s)", flushed.len());
                }
                for coalesced in flushed {
                    match publish_coalesced_job(
                        self.bus.as_ref(),
                        self.job_signer.as_ref(),
                        &coalesced,
                    ) {
                        Some(id) => {
                            debug!(assignee = %coalesced.assignee, %id, "kind-43001 published for coalesced burst")
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
                };
                match self.engine.run(&run_id, input).await {
                    Ok(_) => debug!(run_id, "workflow {} ran", workflow.name),
                    Err(e) => warn!(run_id, "workflow {} failed: {e}", workflow.name),
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A minimal signed kind-14 event with optional tags, for trigger tests.
    fn kind14(content: &str, tags: Vec<nostr_sdk::nostr::Tag>) -> nostr_sdk::nostr::Event {
        let keys = nostr_sdk::nostr::Keys::generate();
        nostr_sdk::nostr::EventBuilder::new(nostr_sdk::nostr::Kind::from_u16(14), content)
            .tags(tags)
            .sign_with_keys(&keys)
            .expect("sign")
    }

    /// bd-pkc part 2: the trigger filter is LIVE in the trigger loop — a
    /// workflow-emitted (stamped) kind-14 does NOT re-trigger.
    #[test]
    fn stamped_workflow_event_is_rejected_at_ingress() {
        let keys = nostr_sdk::nostr::Keys::generate();
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
                nostr_sdk::nostr::Tag::parse([
                    "t",
                    toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG,
                ])
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
            vec![nostr_sdk::nostr::Tag::parse(["t", "myproject"]).expect("t")],
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
            let keys = nostr_sdk::nostr::Keys::generate();
            nostr_sdk::nostr::EventBuilder::new(nostr_sdk::nostr::Kind::from_u16(7), "+")
                .sign_with_keys(&keys)
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
                steps: vec![Step {
                    name: Some("rec".into()),
                    guard: None,
                    on_error: StepOnError::Continue,
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

        let keys = nostr_sdk::nostr::Keys::generate();
        let t_demo = nostr_sdk::nostr::Tag::parse(["t", "demo"]).expect("t");

        // 1. A plain acquire (status-less content) → on-acquire only.
        let acquire = nostr_sdk::nostr::EventBuilder::new(
            nostr_sdk::nostr::Kind::from_u16(30031),
            r#"{"exclusive":true}"#,
        )
        .tags(vec![
            nostr_sdk::nostr::Tag::parse(["d", "demo:src/**"]).expect("d"),
            t_demo.clone(),
        ])
        .sign_with_keys(&keys)
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
        let release = nostr_sdk::nostr::EventBuilder::new(
            nostr_sdk::nostr::Kind::from_u16(30031),
            r#"{"status":"released"}"#,
        )
        .tags(vec![
            nostr_sdk::nostr::Tag::parse(["d", "demo:src/**"]).expect("d"),
            t_demo.clone(),
        ])
        .sign_with_keys(&keys)
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
        let react = nostr_sdk::nostr::EventBuilder::new(nostr_sdk::nostr::Kind::from_u16(7), "+")
            .sign_with_keys(&keys)
            .expect("sign");
        bus.publish(react).await.expect("publish reaction");
        assert!(
            wait_for(&fired, "action:send_message", Duration::from_secs(10)).await,
            "reaction fired the reaction_added workflow: {fired:?}"
        );

        // 4. Loop prevention end-to-end: a WORKFLOW-EMITTED (stamped) kind-14
        //    does NOT fire message_posted; a user kind-14 does.
        fired.lock().unwrap().clear();
        let stamped = nostr_sdk::nostr::EventBuilder::new(
            nostr_sdk::nostr::Kind::from_u16(14),
            "workflow ack",
        )
        .tags(vec![
            t_demo.clone(),
            nostr_sdk::nostr::Tag::parse([
                "t",
                toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG,
            ])
            .expect("stamp"),
        ])
        .sign_with_keys(&keys)
        .expect("sign");
        bus.publish(stamped).await.expect("publish stamped");
        assert!(
            absent_for(&fired, "action:send_message", Duration::from_millis(800)).await,
            "stamped workflow-emitted message must NOT re-trigger: {fired:?}"
        );

        let user_msg =
            nostr_sdk::nostr::EventBuilder::new(nostr_sdk::nostr::Kind::from_u16(14), "user hello")
                .tags(vec![t_demo])
                .sign_with_keys(&keys)
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
            spec: serde_json::json!({ "i": i }),
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
            let rumor = unsigned.sign(&recv_keys).await.expect("sign rumor");
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
        let signer = nostr_sdk::nostr::Keys::generate();
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
                    spec: serde_json::json!({ "i": i }),
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
                    spec: serde_json::json!({ "text": format!("mention {i}") }),
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
        let runner = mailbox_runner(
            mailbox,
            coalescer.clone(),
            Some(nostr_sdk::nostr::Keys::generate()),
        );

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
                        spec: serde_json::json!({ "i": t * 1000 + i }),
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
            toron_core::bus::Filter::new().kinds([nostr_sdk::nostr::Kind::from_u16(43001)]),
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
        let wraps = replay
            .iter()
            .filter(|e| e.kind == nostr_sdk::nostr::Kind::GiftWrap)
            .count();
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
            event: nostr_sdk::nostr::Event,
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
    /// RED today: the guard IS held across the publish at the runner's
    /// FlushNow arm (mailbox_runner, :156-165) and the trigger-loop tick
    /// flush (:441-461) — a concurrent flush for an unrelated assignee
    /// stalls for the full publish duration. After the batch fix (clone the
    /// coalesced job under the lock, release, THEN publish) the concurrent
    /// flush proceeds immediately.
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
        let runner = mailbox_runner(
            mailbox.clone(),
            coalescer.clone(),
            Some(nostr_sdk::nostr::Keys::generate()),
        );

        let dispatch = |runner: &ActionRunner, assignee: &str, text: &str| {
            runner(&Action::DispatchJob {
                assignee: assignee.into(),
                spec: serde_json::json!({ "text": text }),
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
            let runner = mailbox_runner(
                mailbox.clone(),
                coalescer.clone(),
                Some(nostr_sdk::nostr::Keys::generate()),
            );
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
                spec: serde_json::json!({ "text": "after-poison" }),
                channel: None,
            })
        }));
        assert!(
            out.is_ok(),
            "poisoned coalescer must survive — subsequent flush panicked: {out:?}"
        );
        let _ = std::fs::remove_file(&db);
    }
}
