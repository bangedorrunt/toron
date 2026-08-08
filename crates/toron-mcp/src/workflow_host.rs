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
use toron_core::mailbox::{Mailbox, SendMessage};
use toron_workflow::coalesce::{CoalesceDecision, Coalesced, MentionCoalescer};
use toron_workflow::engine::{ActionRunner, RunInput, WorkflowEngine};
use toron_workflow::jobs::{JobRequest, build_job_request_with_tags};
use toron_workflow::schema::{Action, Trigger, Workflow};
use toron_workflow::trigger_filter::should_evaluate_trigger_event;
use toron_workflow::stamp_workflow_emitted;
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
                let mut c = coalescer.lock().expect("coalescer lock");
                match c.submit(mention, now) {
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
    let event = build_job_request_with_tags(keys, parent, &payload, Timestamp::now(), stamped)
        .ok()?;
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
        let engine = WorkflowEngine::local(runner).context("workflow engine")?;
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

    /// Drive the trigger loop to completion (until the subscription closes).
    pub async fn run_trigger_loop(&self) -> Result<()> {
        let mut sub = self
            .bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .map_err(|e| anyhow::anyhow!("bus subscribe: {e}"))?;
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
                let mut c = self.coalescer.lock().expect("coalescer lock");
                let flushed = c.tick(now_ms());
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
                let run_id = format!("wf-{}-{}", workflow.name, event.id.to_hex());
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

/// Match a trigger spec against an inbound event (kind-level).
fn matches_trigger(trigger: &Trigger, event: &nostr_sdk::nostr::Event) -> bool {
    // The trigger filter has already excluded workflow/job/gift-wrap kinds.
    // Match the ADR trigger kinds against the event kind.
    let kind = event.kind.as_u16();
    match trigger {
        Trigger::MessagePosted { .. } => kind == 14 || kind == 9 || kind == 1,
        Trigger::ReactionAdded { .. } => kind == 7,
        Trigger::ReservationAcquired { .. } => kind == 30031,
        Trigger::ReservationReleased { .. } => kind == 30031,
        Trigger::AgentStateChanged { .. } => kind == 20001,
        Trigger::Schedule { .. } => false, // cron-managed, not event-driven
        Trigger::Webhook { .. } => false,  // external, not a bus event
    }
}

/// Build the `{{trigger.*}}` payload from an event.
fn trigger_payload(event: &nostr_sdk::nostr::Event) -> serde_json::Value {
    use serde_json::json;
    let mut m = serde_json::Map::new();
    m.insert("id".into(), json!(event.id.to_hex()));
    m.insert("sender".into(), json!(event.pubkey.to_hex()));
    m.insert("content".into(), json!(event.content));
    m.insert("kind".into(), json!(event.kind.as_u16()));
    m.insert("created_at".into(), json!(event.created_at.as_secs()));
    // `t`/`h`/`importance` tag surfaces for project/channel matching.
    let mut project: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut importance: Option<String> = None;
    let mut tags: Vec<serde_json::Value> = Vec::new();
    for t in event.tags.iter() {
        let p = t.as_slice();
        tags.push(json!(p));
        if p.first().map(String::as_str) == Some("t") && project.is_none() {
            project = p.get(1).cloned();
        }
        if p.first().map(String::as_str) == Some("h") && channel.is_none() {
            channel = p.get(1).cloned();
        }
        if p.first().map(String::as_str) == Some("importance") && importance.is_none() {
            importance = p.get(1).cloned();
        }
    }
    m.insert("tags".into(), serde_json::Value::Array(tags));
    if let Some(p) = project {
        m.insert("project".into(), json!(p));
    }
    if let Some(c) = channel {
        m.insert("channel".into(), json!(c));
    }
    if let Some(i) = importance {
        m.insert("importance".into(), json!(i));
    }
    serde_json::Value::Object(m)
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
        assert!(should_evaluate_trigger_event(&user), "user message evaluates");
        // The same kind-14 carrying the workflow-emitted stamp → rejected.
        let stamped = kind14(
            "hello",
            vec![nostr_sdk::nostr::Tag::parse([
                "t",
                toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG,
            ])
            .expect("stamp tag")],
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
        assert_eq!(payload["sender"].as_str(), Some(ev.pubkey.to_hex().as_str()));
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
            nostr_sdk::nostr::EventBuilder::new(
                nostr_sdk::nostr::Kind::from_u16(7),
                "+",
            )
            .sign_with_keys(&keys)
            .expect("sign")
        };
        assert!(matches_trigger(
            &Trigger::MessagePosted { channel: None, project: None },
            &msg
        ));
        assert!(!matches_trigger(
            &Trigger::ReactionAdded { channel: None, emoji: None },
            &msg
        ));
        assert!(matches_trigger(
            &Trigger::ReactionAdded { channel: None, emoji: None },
            &react
        ));
        // A schedule trigger is never event-driven.
        assert!(!matches_trigger(&Trigger::Schedule { cron: "* * * * *".into(), owner: None, scope: Default::default() }, &msg));
    }

    /// bd-pkc part 2 + part 3: the mailbox runner coalesces DispatchJob @mentions
    /// (Ship 1) — a burst lands in one coalescer buffer, not N jobs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_job_actions_coalesce_via_runner() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-coal-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("connect");
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
        let c = coalescer.lock().expect("lock");
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
        let pool = schema::connect(db.to_str().unwrap()).await.expect("connect");
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
        let recv_keys = toron_core::keys::load_encrypted_key(
            std::path::Path::new(&recv.key_path),
            PASSPHRASE,
        )
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
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sub.next(),
                )
                .await
                {
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
        std::thread::spawn(move || drop(rt)).join().expect("drop rt");
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
        let pool = schema::connect(db.to_str().unwrap()).await.expect("connect");
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
        let mut sub = bus
            .subscribe(toron_core::bus::Filter::new())
            .await
            .expect("subscribe");
        let rt = Arc::new(tokio::runtime::Runtime::new().expect("runner runtime"));
        let mut published = 0;
        for i in 0..3 {
            let runner = runner.clone();
            let rt = rt.clone();
            let out = std::thread::spawn(move || {
                let _guard = rt.enter();
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

        // Host path: a 5-mention batch → one stamped 43001. publish is sync
        // (block_ons the bus — F8); drive it on a plain thread.
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
        let rt_for_publish = rt.clone();
        let id = std::thread::spawn(move || {
            let _guard = rt_for_publish.enter();
            publish_coalesced_job(bus_for_thread.as_ref(), Some(&signer_for_thread), &batch)
                .expect("host publish")
        })
        .join()
        .expect("publish thread");
        assert!(!id.to_hex().is_empty());

        // Drain the bus: exactly two 43001s total (one cap-flush + one host).
        let mut jobs = Vec::new();
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(3), sub.next()).await {
                Ok(Some(SubscriptionEvent::Event(e))) => {
                    jobs.push(e);
                    if jobs.len() >= 4 {
                        break;
                    }
                }
                Ok(Some(SubscriptionEvent::Eose)) | Ok(None) => continue,
                Ok(Some(SubscriptionEvent::Closed(_))) => panic!("subscription closed"),
                Err(_) => break, // quiet window — drain done
            }
        }
        let jobs43001: Vec<_> = jobs
            .iter()
            .filter(|e| e.kind.as_u16() == 43001)
            .cloned()
            .collect();
        assert_eq!(
            jobs43001.len(),
            2,
            "one 43001 per burst, never N (saw kinds {:?})",
            jobs.iter().map(|e| e.kind.as_u16()).collect::<Vec<_>>()
        );
        for ev in &jobs43001 {
            assert_eq!(ev.kind.as_u16(), 43001, "job request kind");
            let stamped = ev.tags.iter().any(|t| {
                t.as_slice().first().map(String::as_str) == Some("t")
                    && t.as_slice().get(1).map(String::as_str)
                        == Some(toron_workflow::trigger_filter::WORKFLOW_EMITTED_TAG)
            });
            assert!(stamped, "43001 carries the workflow-emitted stamp");
            assert!(
                !should_evaluate_trigger_event(ev),
                "stamped 43001 must not re-trigger"
            );
        }
        let _ = std::fs::remove_file(&db);
        // A runtime cannot be dropped inside an async context — drop it on a
        // plain thread (shutdown does blocking work).
        std::thread::spawn(move || drop(rt)).join().expect("drop rt");
    }

    /// End-to-end: a workflow with a `message_posted` trigger + a satisfiable
    /// guard is INVOKED by the trigger loop when a matching event arrives
    /// (Ship 2 filter + Ship 4 guard + engine invoke, all LIVE).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn trigger_loop_invokes_matching_workflow() {
        use toron_core::bus::in_process::{InProcessBus, InProcessBusConfig};
        use toron_core::index::schema;

        let db = std::env::temp_dir().join(format!("wf-host-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db.to_str().unwrap()).await.expect("connect");
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
}

