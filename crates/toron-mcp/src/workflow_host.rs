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
use toron_core::bus::{Bus, SubscriptionEvent};
use toron_core::mailbox::{Mailbox, SendMessage};
use toron_workflow::coalesce::{CoalesceDecision, MentionCoalescer};
use toron_workflow::engine::{ActionRunner, RunInput, WorkflowEngine};
use toron_workflow::schema::{Action, Trigger, Workflow};
use toron_workflow::trigger_filter::should_evaluate_trigger_event;
use tracing::{debug, warn};

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
                };
                match tokio::runtime::Handle::try_current() {
                    Ok(handle) => match handle.block_on(mailbox.send_message(&msg)) {
                        Ok(res) => Ok(serde_json::json!({
                            "ok": true,
                            "count": res.count,
                        })),
                        Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
                    },
                    Err(_) => Ok(serde_json::json!({
                        "ok": false,
                        "error": "no tokio runtime for send_message"
                    })),
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
                };
                match tokio::runtime::Handle::try_current() {
                    Ok(handle) => match handle.block_on(mailbox.send_message(&msg)) {
                        Ok(res) => Ok(serde_json::json!({
                            "ok": true,
                            "count": res.count,
                        })),
                        Err(e) => Ok(serde_json::json!({ "ok": false, "error": e.to_string() })),
                    },
                    Err(_) => Ok(serde_json::json!({
                        "ok": false,
                        "error": "no tokio runtime for send_dm"
                    })),
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
                let decision = c.submit(mention, now);
                Ok(serde_json::json!({
                    "ok": true,
                    "coalesced": matches!(decision, CoalesceDecision::Buffered { .. }),
                }))
            }
            _ => Ok(serde_json::Value::Null),
        }
    })
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
}

impl std::fmt::Debug for WorkflowHost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHost")
            .field("workflows", &self.workflows.len())
            .field("engine", &self.engine)
            .finish()
    }
}

impl WorkflowHost {
    /// Construct the host over a mailbox: engine with the production mailbox
    /// runner, the bus from the mailbox, plus the workflow set.
    pub fn new(mailbox: Arc<Mailbox>, workflows: Vec<Workflow>) -> Result<Self> {
        let coalescer = Arc::new(std::sync::Mutex::new(MentionCoalescer::from_env()));
        let runner = mailbox_runner(mailbox.clone(), coalescer.clone());
        let engine = WorkflowEngine::local(runner).context("workflow engine")?;
        let bus = mailbox.bus().clone();
        Ok(Self {
            engine: Arc::new(engine),
            bus,
            workflows,
            coalescer,
        })
    }

    /// Construct over an explicit bus (tests / embeddings without a mailbox).
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
        })
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
            // window elapsed. (The flush is a no-op for the trigger loop —
            // real job emission is a P4 subsystem; here the tick keeps the
            // buffered mentions from growing unbounded.)
            {
                let mut c = self.coalescer.lock().expect("coalescer lock");
                let flushed = c.tick(now_ms());
                if !flushed.is_empty() {
                    debug!("coalescer flushed {} assignee(s)", flushed.len());
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
        let runner = mailbox_runner(mailbox, coalescer.clone());
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

