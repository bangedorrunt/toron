// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-9 — @mention → job claim → result (P2 / Phase-2 gate).
//!
//! Actors: human H in channel #forge, OC-β, Grok-γ (members).
//!
//! 1. H posts "@OCβ fix the flaky test" (kind-14 DM carrying p=OC-β — the
//!    channel-agnostic @mention path from bd-yt2; the job's parent is this
//!    message).
//! 2. OC-β's subscriber gets the mention via BUS PUSH (no polling) → dispatches
//!    a job request (kind-43001) + OC-β claims it (kind-43002, first-claimer
//!    wins — signed).
//! 3. OC-β posts a result (kind-43004) → lifecycle closes.
//! 4. Grok-γ (mentioned nowhere) does NOT receive the job, but sees the thread
//!    (the trigger message) — isolation of the job to the mentioned agent.
//!
//! Gate: `cargo test -p toron-e2e --features p2`.
#![cfg(feature = "p2")]

use std::sync::Arc;
use std::time::Instant;

use nostr_sdk::prelude::*;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::bus::{Bus, Filter, InProcessBus, SubscriptionEvent};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::nostr::identity::AgentProfile;
use toron_workflow::jobs::{
    build_job_claim, build_job_request, build_job_result, first_claimer, job_parent, job_state,
    JobRequest, JobResult, JobState,
};

async fn boot(dir: &std::path::Path) -> (Mailbox, InProcessBus) {
    let db = dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    schema::migrate(&pool).await.expect("migrate");
    let bus = InProcessBus::open(
        pool.clone(),
        InProcessBusConfig {
            auth: AuthPolicy::AcceptAll,
            allowlist: vec![],
            channel_capacity: 256,
            detailed_stage_logging: false,
        },
    )
    .await
    .expect("bus");
    let mailbox = Mailbox::new(Arc::new(bus.clone()), dir.to_path_buf(), None)
        .with_agent_passphrase("toron-dev")
        .with_index_pool(pool);
    (mailbox, bus)
}

async fn register(mailbox: &Mailbox, project: &str, name: &str) -> PublicKey {
    mailbox
        .register_agent(project, &AgentProfile::new(name))
        .await
        .expect("register")
        .npub
        .parse()
        .expect("npub")
}

#[tokio::test]
async fn sc9_mention_to_job_claim_to_result() {
    let dir = std::env::temp_dir().join(format!("toron-sc9-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let (mailbox, bus) = boot(&dir).await;

    let _h_pk = register(&mailbox, "forge", "Human").await;
    let oc_pk = register(&mailbox, "forge", "OCBeta").await;
    let grok_pk = register(&mailbox, "forge", "GrokGamma").await;

    // OC-β subscribes to its own gift wraps BEFORE the mention (push path).
    let mut oc_sub = bus
        .subscribe(Filter::new().kinds([Kind::GiftWrap]).pubkey(oc_pk))
        .await
        .expect("OC subscribes to wraps");
    // Grok-γ also subscribes (must NOT receive the job).
    let mut grok_sub = bus
        .subscribe(Filter::new().kinds([Kind::GiftWrap]).pubkey(grok_pk))
        .await
        .expect("Grok subscribes to wraps");

    // 1. H posts "@OCβ fix the flaky test" → kind-14 DM to OC-β (the mention
    //    triggers a job; the message is the job's parent).
    let sent = mailbox
        .send_message(&toron_core::mailbox::messaging::SendMessage {
            project_slug: "forge".into(),
            sender_name: "Human".into(),
            to: vec!["OCBeta".into()],
            subject: "fix it".into(),
            body_md: "@OCBeta fix the flaky test".into(),
            ..Default::default()
        })
        .await
        .expect("H mentions OC-β");
    assert_eq!(sent.count, 1, "one delivery to OC-β");
    let parent_id = EventId::from_hex(&sent.deliveries[0].payload.event_id).expect("parent id");

    // OC-β receives the mention via bus push (<50ms) — the message body is the
    // @mention; the kind-14 gift wrap unwraps to the rumor that carries the
    // mention (parse after unwrap via the mailbox inbox).
    let t0 = Instant::now();
    let _push = tokio::time::timeout(std::time::Duration::from_millis(100), async {
        loop {
            match oc_sub.next().await {
                Some(SubscriptionEvent::Event(evt)) => return Some(evt),
                Some(SubscriptionEvent::Eose) => continue,
                Some(SubscriptionEvent::Closed(_)) | None => return None,
            }
        }
    })
    .await
    .expect("OC receives push")
    .expect("push arrives");
    assert!(
        t0.elapsed().as_millis() < 50,
        "OC push {:?} < 50ms (no polling)",
        t0.elapsed()
    );

    // The @mention is present on the unwrapped rumor (channel-agnostic
    // parser — p tag + @token in content).
    let inbox = mailbox
        .fetch_inbox(&toron_core::mailbox::messaging::FetchInbox {
            project_slug: "forge".into(),
            agent_name: "OCBeta".into(),
            include_bodies: true,
            ..Default::default()
        })
        .await
        .expect("OC inbox");
    let mention_msg = inbox
        .iter()
        .find(|m| m.body_md.contains("@OCBeta"))
        .expect("mention message in OC inbox");
    assert!(mention_msg.body_md.contains("fix the flaky test"));

    // Build the job request (kind-43001) referencing the parent mention.
    let oc_keys = Keys::generate(); // OC-β signing key
    let job_req = build_job_request(
        &oc_keys,
        parent_id,
        &JobRequest::new(
            "fix the flaky test",
            serde_json::json!({"assignee": "OCBeta"}),
        ),
        Timestamp::now(),
    )
    .expect("job request");
    assert_eq!(job_req.kind.as_u16(), 43001, "@mention creates kind-43001");
    assert_eq!(
        job_parent(&job_req),
        Some(parent_id),
        "job e-tags the mention"
    );
    bus.publish(job_req.clone())
        .await
        .expect("publish job request");

    // 2. OC-β claims it (kind-43002, signed). Grok-γ claims concurrently but
    //    later → first-claimer wins.
    let oc_claim = build_job_claim(&oc_keys, job_req.id, Timestamp::now()).expect("OC claim");
    assert_eq!(oc_claim.kind.as_u16(), 43002, "claim signed");
    let grok_keys = Keys::generate(); // Grok-γ signing key
    let grok_claim = build_job_claim(
        &grok_keys,
        job_req.id,
        Timestamp::from(Timestamp::now().as_secs() + 1),
    )
    .expect("Grok claim (later)");
    bus.publish(oc_claim.clone()).await.expect("OC claims");
    bus.publish(grok_claim).await.expect("Grok claims later");

    // First-claimer = OC-β (observed first on the InProcessBus total order).
    let all = bus
        .subscribe(Filter::new().kinds([Kind::from_u16(43002)]))
        .await
        .expect("claims sub");
    let mut events = Vec::new();
    let mut sub = all;
    while let Some(ev) = sub.next().await {
        match ev {
            SubscriptionEvent::Event(e) => events.push(e),
            _ => break,
        }
    }
    let winner = first_claimer(job_req.id, &events).expect("a claimer won");
    assert_eq!(
        winner,
        oc_claim.pubkey.to_hex(),
        "first claimer (OC-β, signed first) wins, not Grok-γ"
    );
    assert_ne!(winner, grok_pk.to_hex(), "later claimer must lose");

    // 3. OC-β posts the result (kind-43004) → lifecycle closes.
    let oc_result = build_job_result(
        &oc_keys,
        job_req.id,
        &JobResult {
            status: "ok".into(),
            body: "fixed".into(),
        },
        Timestamp::now(),
    )
    .expect("OC result");
    assert_eq!(
        oc_result.kind.as_u16(),
        43004,
        "result closes the lifecycle"
    );
    bus.publish(oc_result.clone())
        .await
        .expect("publish result");

    let lifecycle_events = bus
        .subscribe(Filter::new().custom_tag(
            nostr_sdk::nostr::SingleLetterTag::lowercase(nostr_sdk::nostr::Alphabet::E),
            job_req.id.to_hex(),
        ))
        .await
        .expect("lifecycle sub");
    let mut lc = Vec::new();
    let mut lsub = lifecycle_events;
    while let Some(ev) = lsub.next().await {
        match ev {
            SubscriptionEvent::Event(e) => lc.push(e),
            _ => break,
        }
    }
    assert_eq!(
        job_state(job_req.id, &lc),
        JobState::Done("ok".into()),
        "lifecycle reaches Done(ok)"
    );

    // 4. Grok-γ does NOT receive the job (no gift wrap addressed to it), but
    //    the trigger message is visible in the project (the thread).
    let grok_got_job = tokio::time::timeout(std::time::Duration::from_millis(50), async {
        loop {
            match grok_sub.next().await {
                Some(SubscriptionEvent::Event(_)) => return true,
                Some(SubscriptionEvent::Eose) => continue,
                Some(SubscriptionEvent::Closed(_)) | None => return false,
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        !grok_got_job,
        "Grok-γ (not mentioned) must NOT receive the job wrap"
    );

    // The mention message is visible in the project (thread visibility).
    assert!(mention_msg.body_md.contains("@OCBeta"));

    let _ = std::fs::remove_dir_all(&dir);
}
