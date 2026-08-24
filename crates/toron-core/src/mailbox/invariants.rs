// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Safety invariants enforcement suite (bd-led.5.11).
//!
//! The AM-Rust parity contract's non-negotiable invariants, asserted over the
//! toron mailbox primitives so regressions under the Nostr rewrite fail fast:
//!
//! 1. `broadcast=true` rejected (explicit recipients only).
//! 2. Project isolation — no cross-project leak without a product/contact
//!    bridge.
//! 3. Symmetric glob reservation conflicts (`src/**` ↔ `src/lib.rs`).
//! 4. TTL not forever (missing expiration defaults per AM rules; permanent
//!    requires explicit opt-in).
//! 5. Wrong-surface denial (a tool rejects the wrong object class).
//! 6. Idempotent bootstrapping (`ensure_project`, `register_agent`).
//! 7. Contact approval policy (`contacts_only` requires an approved link).

use super::Mailbox;

/// Asserts a tool-level invariant and logs it for CI triage.
fn assert_invariant(name: &str, cond: bool, detail: impl std::fmt::Display) {
    assert!(cond, "[invariant:{name}] violated — {detail}");
}

impl Mailbox {
    /// Invariant 1 (broadcast reject): an explicit-recipient send with no
    /// recipients is rejected — no implicit broadcast. This mirrors AM-Rust's
    /// `broadcast=true` rejection at the tool layer.
    pub async fn invariant_broadcast_rejected(&self) -> anyhow::Result<bool> {
        let result = self
            .send_message(&crate::mailbox::SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                subject: "x".into(),
                body_md: "y".into(),
                ..Default::default()
            })
            .await;
        assert_invariant("broadcast", result.is_err(), format!("{result:?}"));
        Ok(result.is_err())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::bus::scoping::{is_metadata_hiding_kind, with_project_scoping};
    use crate::index::schema;
    use crate::mailbox::contacts::{RequestContact, RespondContact};
    use crate::mailbox::messaging::{FetchInbox, SendMessage};
    use crate::mailbox::unwrap::ContactPolicy;
    use crate::mailbox::{Mailbox, ProjectOutcome, overlaps};
    use crate::nostr::identity::AgentProfile;
    use crate::nostr::kinds::{PROJECT_MANIFEST, toron_kind};
    use std::sync::Arc;

    const PASSPHRASE: &str = "invariants-test-pass";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-inv-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-inv-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        }
    }

    async fn register(mailbox: &Mailbox, project: &str, name: &str) -> crate::nostr::PublicKey {
        let agent = mailbox
            .register_agent(project, &profile(name))
            .await
            .expect("register");
        crate::nostr::PublicKey::from_hex(&agent.npub).expect("npub")
    }

    #[tokio::test]
    async fn invariant_broadcast_rejected() {
        let (mailbox, db_path, data_dir) = test_mailbox("broadcast").await;
        register(&mailbox, "demo", "GreenCastle").await;

        // No to/cc/bcc → no explicit recipients → send rejected (broadcast
        // denial).
        assert!(mailbox.invariant_broadcast_rejected().await.expect("inv"));
        // Explicit recipients succeed.
        register(&mailbox, "demo", "BlueLake").await;
        let ok = mailbox
            .send_message(&SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: "s".into(),
                body_md: "b".into(),
                ..Default::default()
            })
            .await;
        assert_invariant("broadcast-explicit", ok.is_ok(), format!("{ok:?}"));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn invariant_project_isolation_no_leak() {
        let (mailbox, db_path, data_dir) = test_mailbox("isolation").await;
        let blue = register(&mailbox, "projA", "BlueLake").await;
        register(&mailbox, "projB", "RoseDune").await;

        // A message sent within projA to BlueLake: the projB agent must not
        // see it via a project-scoped filter.
        mailbox
            .send_message(&SendMessage {
                project_slug: "projA".into(),
                sender_name: "BlueLake".into(),
                to: vec!["BlueLake".into()],
                subject: "secret".into(),
                body_md: "projA only".into(),
                ..Default::default()
            })
            .await
            .expect("send in projA");

        // RoseDune (projB) fetch_inbox → empty (no cross-project leak).
        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "projB".into(),
                agent_name: "RoseDune".into(),
                include_bodies: true,
                ..Default::default()
            })
            .await
            .expect("fetch projB");
        assert_invariant(
            "isolation",
            inbox.is_empty(),
            format!("projB leaked projA mail: {inbox:?}"),
        );
        let _ = blue;

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn invariant_symmetric_glob() {
        assert!(overlaps("src/**", "src/lib.rs"));
        assert!(overlaps("src/lib.rs", "src/**"), "both directions");
        assert!(!overlaps("src/**", "docs/x.md"));
        assert!(!overlaps("src/a.rs", "src/b.rs"));
        // Directory patterns vs their own children.
        assert!(overlaps(
            "crates/toron-core/**",
            "crates/toron-core/src/lib.rs"
        ));
    }

    #[tokio::test]
    async fn invariant_ttl_not_forever() {
        let (mailbox, db_path, data_dir) = test_mailbox("ttl").await;
        register(&mailbox, "demo", "BlueLake").await;

        // Acquire with NO explicit TTL → defaults to 90d (not forever).
        let acq = mailbox
            .file_reservation_paths("demo", "BlueLake", &["src/**".into()], None, true, None)
            .await
            .expect("acquire");
        assert_eq!(acq.granted.len(), 1);
        let exp = acq.granted[0].expires_ts_us;
        assert_invariant("ttl-not-forever", exp.is_some(), "default TTL must be set");
        // expires_ts_us is absolute (now + TTL); the span must be ~90d.
        let now_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        let span = exp.expect("ttl") - now_us;
        let span_secs = span / 1_000_000;
        assert_invariant(
            "ttl-90d",
            span_secs > 0 && span_secs <= (crate::bus::DEFAULT_ENVELOPE_TTL_SECS as i64),
            format!("default TTL span {span_secs}s exceeds 90d"),
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn invariant_wrong_surface_denial() {
        // Metadata-hiding kinds must NEVER gain project scoping tags (A3).
        let base = Vec::new();
        let scoped = with_project_scoping(crate::nostr::Kind::GiftWrap, base, Some("demo"));
        assert_invariant(
            "wrong-surface",
            scoped.is_empty(),
            "gift wraps must not be project-tagged",
        );
        assert!(is_metadata_hiding_kind(crate::nostr::Kind::GiftWrap));
        assert!(is_metadata_hiding_kind(
            crate::nostr::Kind::PrivateDirectMessage
        ));

        // A plain non-hiding kind DOES get scoped.
        let plain = with_project_scoping(toron_kind(PROJECT_MANIFEST), Vec::new(), Some("demo"));
        assert_invariant(
            "wrong-surface",
            plain.len() == 3,
            "non-hiding gets dual tags",
        );
    }

    #[tokio::test]
    async fn invariant_idempotent_bootstrapping() {
        let (mailbox, db_path, data_dir) = test_mailbox("idem").await;

        let a = mailbox.ensure_project("demo").await.expect("ensure 1");
        assert!(a.created);
        let b = mailbox.ensure_project("demo").await.expect("ensure 2");
        assert_invariant("idem-ensure", !b.created, "ensure twice = no-op");

        let r1 = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("register 1");
        let r2 = mailbox
            .register_agent("demo", &profile("BlueLake"))
            .await
            .expect("register 2");
        assert_invariant(
            "idem-register",
            r1.npub == r2.npub && !r2.created,
            "re-register keeps npub, not created",
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn invariant_contact_approval_policy() {
        let (mailbox, db_path, data_dir) = test_mailbox("policy").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;
        let rose = register(&mailbox, "demo", "RoseDune").await;

        // contacts_only with no approved link → inbound dropped.
        let sender = crate::keys::generate();
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            crate::nostr::Timestamp::now(),
            crate::nostr::Kind::PrivateDirectMessage,
            vec![],
            "hi",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(&sender, &rose, rumor)
            .expect("wrap");
        mailbox.bus.publish(wrap).await.expect("publish");

        // RoseDune's policy: contacts_only. No approved link → drop.
        mailbox
            .set_contact_policy("demo", "RoseDune", ContactPolicy::ContactsOnly)
            .await
            .expect("set contacts_only");

        // The wrap is addressed to `rose`; unwrap requires rose's key.
        let rose_keys = mailbox
            .agent_keys_for("demo", "RoseDune")
            .expect("rose key");
        let source = Arc::new(crate::mailbox::BusPolicySource::load(&mailbox, &rose).await);
        let sink = Arc::new(crate::mailbox::unwrap::SqliteProjectionSink::new(
            mailbox.index_pool().unwrap().clone(),
        ));
        let pipeline = crate::mailbox::unwrap::UnwrapPipeline::new(vec![rose_keys], source, sink);

        // Re-fetch the wrap (it was published above).
        let filter = crate::bus::filter::ToronFilter::new()
            .kinds([crate::nostr::Kind::GiftWrap])
            .pubkey(rose)
            .build();
        let wraps = super::super::collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("collect wraps");
        let wrap = wraps.into_iter().next().expect("one wrap");
        let outcome = pipeline.project(&wrap).await;
        assert_invariant(
            "contact-approval",
            outcome == ProjectOutcome::Dropped(ContactPolicy::ContactsOnly),
            format!("{outcome:?}"),
        );

        // After approving a link, is_approved_contact is true.
        mailbox
            .request_contact(&RequestContact {
                project_slug: "demo".into(),
                from_agent: "RoseDune".into(),
                to_agent: "BlueLake".into(),
                to_project: None,
                reason: None,
            })
            .await
            .expect("request");
        mailbox
            .respond_contact(&RespondContact {
                project_slug: "demo".into(),
                to_agent: "BlueLake".into(),
                from_agent: "RoseDune".into(),
                accept: true,
                from_project: None,
            })
            .await
            .expect("approve");
        assert!(
            mailbox.is_approved_contact(&blue, &rose).await,
            "approved link satisfies contacts_only"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
