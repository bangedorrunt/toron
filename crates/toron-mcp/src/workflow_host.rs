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
//!   evaluates true, is invoked through [`toron_workflow::engine::WorkflowEngine`] (`run`, the same
//!   durable function as the trigger path).
//! - The engine's async effect runner dispatches through the Mailbox (a DM send
//!   emits a kind-14 gift wrap) and STAMPS every event it emits with
//!   `toron:workflow-emitted` — so a workflow can never re-trigger itself.
//! - `@mention` dispatch runs through a [`toron_workflow::coalesce::MentionCoalescer`] (one job per
//!   burst), ticked by the trigger loop's per-event flush.
//!
//! governed-by: ADR-0004 ships 1/2/4/5, ADR-0005 Spec 2

pub use toron_workflow::engine::dispatch::{
    DISPATCH_LEASE_US, DispatchRequest, DispatchResult, EffectDispatcher, classify_outcome,
    durable_route,
};
pub use toron_workflow::engine::host::{
    COALESCE_FLUSH_TICK_MS, CoalescePublishError, DurablePark, ParkCtx, PendingDrain, RunState,
    WorkflowHost, host_acquire_for_assignee, mailbox_async_effect_runner, mention_outcome, now_ms,
    park_candidates, publish_coalesced_job, publish_coalesced_job_with_cursor,
};

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
    use anyhow::Result;
    use nostr::prelude::{Event, EventBuilder, FinalizeEvent, Keys, Kind, Tag};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, PoisonError};
    use toron_core::bus::{Bus, SubscriptionEvent};
    use toron_core::index::workflow_effects::{EffectState, PendingKind, WorkflowEffectStore};
    use toron_core::mailbox::Mailbox;
    use toron_workflow::coalesce::{
        CoalesceDecision, Coalesced, DlqReason, MentionCoalescer, RequeueOutcome, RequeueSchedule,
    };

    use toron_workflow::dispatch::{DispatchAcquire, DispatchConfig, DispatchPool};
    use toron_workflow::engine::armor::{
        ARMOR_MAX_CONCURRENCY, ARMOR_SEND_TIMEOUT, SendArmor, passthrough,
    };
    use toron_workflow::engine::{
        ActionInvocation, AsyncEffectRunner, ParkKind, PendingEntry, ProviderOutcome,
        ResolutionMismatch, WorkflowEngine,
    };
    use toron_workflow::engine::{DispatchPlan, ProviderCapability, effect_view, plan_attempt};
    use toron_workflow::lifecycle::LifecycleEffect;
    use toron_workflow::schema::{Action, Trigger, Workflow};
    use toron_workflow::trigger_filter::{
        matches_trigger, should_evaluate_trigger_event, trigger_payload,
    };

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
            await_: None,
            no_claim_timeout: 600,
            result_timeout: None,
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
                    await_: None,
                    no_claim_timeout: 600,
                    result_timeout: None,
                },
                // One run: the cap-flush property is about ONE key. Distinct
                // runs and distinct nodes are separate jobs by design
                // (bd-6setu.2), covered by coalesce.rs.
                "run-cap-flush",
            )
            .await;
            if out["published"].as_bool() == Some(true) {
                published += 1;
            }
        }
        assert_eq!(published, 1, "cap-flush publishes exactly one 43001");

        let parent = "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b";
        let batch = toron_workflow::coalesce::Coalesced {
            run_id: None,
            node_path: None,
            assignee: "gk".into(),
            mentions: (0..5)
                .map(|i| InboundMention {
                    run_id: None,
                    node_path: None,
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
                            await_: None,
                            no_claim_timeout: 600,
                            result_timeout: None,
                        },
                        // One key for the whole burst: the bound this test
                        // names is coalescing WITHIN a step, not across runs.
                        "run-burst",
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
                await_: None,
                no_claim_timeout: 600,
                result_timeout: None,
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
                await_: None,
                no_claim_timeout: 600,
                result_timeout: None,
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
                    await_: None,
                    no_claim_timeout: 600,
                    result_timeout: None,
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
                    await_: None,
                    no_claim_timeout: 600,
                    result_timeout: None,
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
                    await_: None,
                    no_claim_timeout: 600,
                    result_timeout: None,
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
        use toron_workflow::engine::{Resolution, ResolutionVerdict};
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
            ClaimAttempt, EffectState, NewAttempt, NewEffect, NewPending, TerminalPub,
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
                    timeout_secs: None,
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
                    await_: None,
                    no_claim_timeout: 600,
                    result_timeout: None,
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
            await_: None,
            no_claim_timeout: 600,
            result_timeout: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![
                InboundMention {
                    run_id: None,
                    node_path: None,
                    assignee: session.into(),
                    parent: "parent-a".into(),
                    spec: json!({"text": "a"}),
                },
                InboundMention {
                    run_id: None,
                    node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![
                InboundMention {
                    run_id: None,
                    node_path: None,
                    assignee: session.into(),
                    parent: "parent-a".into(),
                    spec: json!({"text": "a2"}),
                },
                InboundMention {
                    run_id: None,
                    node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
                run_id: None,
                node_path: None,
                assignee: "oc".into(),
                parent: "p1".into(),
                spec: json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
                run_id: None,
                node_path: None,
                assignee: "oc".into(),
                parent: "p-tn".into(),
                spec: json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![
                InboundMention {
                    run_id: None,
                    node_path: None,
                    assignee: session.into(),
                    parent: "already".into(),
                    spec: json!({"n": 1}),
                },
                InboundMention {
                    run_id: None,
                    node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
                    run_id: None,
                    node_path: None,
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
                run_id: None,
                node_path: None,
                assignee: session.into(),
                mentions: vec![InboundMention {
                    run_id: None,
                    node_path: None,
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
            run_id: None,
            node_path: None,
            assignee: session.into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
                run_id: None,
                node_path: None,
                assignee: "oc".into(),
                parent: "live".into(),
                spec: json!({"t": 1}),
            },
            1_000,
        );
        let failed = Coalesced {
            run_id: None,
            node_path: None,
            assignee: "oc".into(),
            mentions: vec![InboundMention {
                run_id: None,
                node_path: None,
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
                run_id: None,
                node_path: None,
                assignee: "oc".into(),
                parent: "tick-p".into(),
                spec: json!({"t": 1}),
            },
            0,
        );
        let batch = c.force_flush("oc").into_iter().next().expect("flushed");
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
                            timeout_secs: None,
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
                            min: None,
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
                    min: None,
                    until: Some("steps_notify_ok == true".into()),
                    body: vec![linear_step()],
                },
            }];
            assert!(durable_route(&wf, true, false).is_ok());

            let mut unbounded = wf.clone();
            unbounded.steps[0].action = Action::Loop {
                max: "{{steps.plan.n}}".into(),
                min: None,
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
}
