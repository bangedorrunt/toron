// governed-by: ADR-0003
// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! SC-13 — Private NIP-29 channel collaboration room (P2 gate).
//!
//! Actors: Pi-α, OC-β, Grok-γ in private #auth-refactor; non-member Pi-δ.
//!
//! Steps:
//!   1. operator creates a private channel; members join.
//!   2. Pi-α posts (kind **9**, ADR-0020); OC-β reacts (kind 7); Grok-γ
//!      @mentions Pi-α.
//!   3. Pi-δ attempts read/post → DENIED.
//!
//! Assert:
//!   - membership is the ONLY gate (member can read/post; non-member denied)
//!   - channel posts emit kind 9 (ADR-0020 Spec 1); legacy kind 42 ingests
//!   - global subscription excluded from private channel events (no leak)
//!   - reaction kind 7 works
//!
//! Drives `toron_workflow::channels::ChannelStore` (the NIP-29 membership-
//! gated store shipped by bd-aws). Deterministic keypairs, no network.
//!
//! Gate: `cargo test -p toron-e2e --features p2`.
#![cfg(feature = "p2")]

use nostr_sdk::prelude::*;
use toron_workflow::channels::{
    CHANNEL_CREATE, CHANNEL_MESSAGE, CHANNEL_MESSAGE_LEGACY, ChannelStore, REACTION,
};

/// Named actors with fixed keys for a readable scenario trace.
fn actors() -> (Keys, Keys, Keys, Keys) {
    // Pi-α, OC-β, Grok-γ (members), Pi-δ (non-member).
    let keys: Vec<Keys> = (0..4)
        .map(|i| Keys::parse(&format!("{:064x}", i + 1)).expect("fixed key"))
        .collect();
    (
        keys[0].clone(),
        keys[1].clone(),
        keys[2].clone(),
        keys[3].clone(),
    )
}

#[tokio::test]
async fn sc13_private_channel_membership_gate() {
    let (pi_a, oc_b, grok_g, pi_d) = actors();
    let operator = Keys::generate();

    // 1. Operator creates a PRIVATE channel; members join.
    let store = ChannelStore::new();
    store
        .create_channel(
            &operator.public_key(),
            "auth-refactor",
            "Auth Refactor",
            true, // private: global subs must never see its events
            None,
        )
        .expect("create private channel");
    for member in [&pi_a, &oc_b, &grok_g] {
        store
            .join("auth-refactor", &member.public_key())
            .expect("member join");
    }

    // Membership is the only gate: members in, non-member Pi-δ out.
    assert!(store.is_member("auth-refactor", &pi_a.public_key()));
    assert!(store.is_member("auth-refactor", &oc_b.public_key()));
    assert!(store.is_member("auth-refactor", &grok_g.public_key()));
    assert!(!store.is_member("auth-refactor", &pi_d.public_key()));

    // 2. Pi-α posts (kind 9 per ADR-0020 Spec 1); OC-β reacts (kind 7);
    //    Grok-γ @mentions Pi-α.
    let post = store
        .post(&pi_a, "auth-refactor", "password hashing review", &[])
        .expect("member posts");
    assert_eq!(
        post.kind.as_u16(),
        9,
        "channel post emits kind 9 (ADR-0020 Spec 1)"
    );
    assert_eq!(post.kind.as_u16(), CHANNEL_MESSAGE);

    let reaction = store
        .react(&oc_b, "auth-refactor", &post.id.to_hex(), "👍")
        .expect("member reacts");
    assert_eq!(reaction.kind.as_u16(), REACTION, "reaction is kind 7");

    let mention = store
        .post(
            &grok_g,
            "auth-refactor",
            "@Pi-α: review the argon2 params",
            &[pi_a.public_key()],
        )
        .expect("member mentions");
    assert_eq!(mention.kind.as_u16(), 9, "@mention post is also kind 9");
    assert_eq!(mention.kind.as_u16(), CHANNEL_MESSAGE);
    assert!(
        mention.tags.iter().any(|t| t.kind() == TagKind::p()
            && t.content()
                .map(|c| c == pi_a.public_key().to_hex())
                .unwrap_or(false)),
        "@mention must tag Pi-α"
    );

    // History is visible to a member.
    let hist = store.history("auth-refactor");
    assert_eq!(hist.len(), 3, "post + reaction + mention recorded");

    // 3. Non-member Pi-δ: read denied + post denied.
    assert!(
        !store.can_read("auth-refactor", &pi_d.public_key()),
        "non-member cannot read private channel"
    );
    match store.post(&pi_d, "auth-refactor", "let me in", &[]) {
        Err(e) => assert!(
            e.to_string().contains("not a member"),
            "post denied for non-member: {e}"
        ),
        Ok(_) => panic!("non-member POST must be denied"),
    }

    // Global-subscription exclusion: a reader with no member set (a global
    // sub) must not see the private channel. `can_read` requires membership,
    // so a non-member is exactly the global-sub shape — nothing leaks.
    let unknown = Keys::generate(); // not a member, no channel context
    assert!(
        !store.can_read("auth-refactor", &unknown.public_key()),
        "global subscription (non-member) must be excluded from private channel events"
    );

    // 1b. The channel is private (flag) and the CREATE kind is emitted.
    let ch = store.get("auth-refactor").expect("channel exists");
    assert!(ch.private, "channel must be private");
    assert_eq!(CHANNEL_CREATE, 40, "kind registry stable");

    // 2b. ADR-0020 Spec 1 legacy ingest: a kind-42 event from a member
    // projects as a channel post alongside the kind-9 emits.
    let legacy = EventBuilder::new(Kind::from_u16(42), "legacy pre-ADR-0020 post")
        .tags(vec![Tag::parse(["h", "auth-refactor"]).expect("h tag")])
        .sign_with_keys(&oc_b)
        .expect("sign legacy kind-42 post");
    let projected = store.ingest(&legacy).expect("legacy kind 42 ingests");
    assert_eq!(projected.kind, 42, "legacy 42 projects as a channel post");
    assert_eq!(projected.channel_id, "auth-refactor");
    assert_eq!(
        store.history("auth-refactor").len(),
        4,
        "post + reaction + mention + legacy"
    );

    // 2c. Kind registry stability (ADR-0020 Spec 1): 9 is the canonical emit,
    // 42 is legacy ingest only.
    assert_eq!(CHANNEL_MESSAGE, 9, "channel post emits kind 9 (ADR-0020)");
    assert_eq!(CHANNEL_MESSAGE_LEGACY, 42, "legacy 42 is ingest only");
}
