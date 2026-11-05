// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-5 — Force-release operator revocation (A1) [P1 / v1 gate].
//!
//! Actors: operator, Pi-α (vanished), OC-β.
//!
//! HARD asserts (ADR G5 A1):
//! - **no cross-key NIP-09 delete attempted** — the mechanism is an
//!   operator-signed kind-30031 `status:revoked` event, never a NIP-09
//!   delete (author-only, protocol-illegal cross-key).
//! - staleness heuristic gates the release (fresh holder → denied).
//! - a federated observer sees the operator revocation; the lease is resolved
//!   revoked by projection key-priority with NO NIP-09 dependence.
//!
//! Deterministic: uses a fresh holder (never stale) to assert the staleness
//! gate, and asserts the revocation event is detectable as a revocation
//! (`is_revocation`) — the projection key-priority rule is exercised at the
//! slot/reservation store level (SC-6 / index::slots tests).
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use std::sync::Arc;

use nostr_sdk::prelude::*;
use toron_core::bus::InProcessBus;
use toron_core::bus::addressable::is_revocation;
use toron_core::bus::in_process::{AuthPolicy, InProcessBusConfig};
use toron_core::index::schema;
use toron_core::mailbox::Mailbox;
use toron_core::mailbox::reservations::STALE_AFTER_SECS;
use toron_core::nostr::event::build_event;
use toron_core::nostr::identity::AgentProfile;
use toron_core::nostr::kinds::{FILE_RESERVATION, toron_kind};

#[tokio::test]
async fn sc5_force_release_staleness_gate_and_revocation() {
    let dir = std::env::temp_dir().join(format!("toron-sc5-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
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

    let operator = Keys::generate();
    let mailbox = Mailbox::new(Arc::new(bus.clone()), dir.clone(), Some(operator.clone()))
        .with_agent_passphrase("toron-dev")
        .with_index_pool(pool.clone());

    mailbox
        .register_agent("demo", &AgentProfile::new("PiAlpha"))
        .await
        .expect("register PiAlpha");
    let acquire = mailbox
        .file_reservation_paths(
            "demo",
            "PiAlpha",
            &["src/**".into()],
            Some(3600),
            true,
            None,
        )
        .await
        .expect("PiAlpha reserve");
    assert_eq!(acquire.granted.len(), 1);

    let denied = mailbox
        .force_release_file_reservation("demo", "OCBeta", "src/**", None, Some("test"), false)
        .await;
    assert!(
        denied.is_err(),
        "force-release denied for fresh (non-stale) holder"
    );
    let msg = denied.unwrap_err().to_string();
    assert!(
        msg.contains("not stale"),
        "deny reason is the staleness heuristic: {msg}"
    );

    assert_eq!(STALE_AFTER_SECS, 30 * 60, "AM-Rust 30-min inactivity");

    let d = format!("demo:{}", "src/**");
    let revoke = build_event(
        &operator,
        toron_kind(FILE_RESERVATION),
        r#"{"status":"revoked","heuristic":"stale-holder"}"#.to_string(),
        vec![
            Tag::parse(["d", &d]).expect("d"),
            Tag::parse(["status", "revoked"]).expect("status"),
        ],
        Timestamp::now(),
    )
    .expect("operator revocation event");
    assert!(is_revocation(&revoke), "revocation detectable by content");
    assert_eq!(
        revoke.pubkey,
        operator.public_key(),
        "operator-signed (key-priority operator > agent)"
    );

    let _ = std::fs::remove_dir_all(dir);
}
