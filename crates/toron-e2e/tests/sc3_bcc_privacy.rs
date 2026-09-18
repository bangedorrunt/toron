// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-3 — to/cc/bcc privacy matrix (P1 / v1 gate).
//!
//! Actors: Pi-α (sender), OC-β (to), Grok-γ (cc), Pi-δ + Pi-ε (bcc).
//!
//! Assert (ADR G3):
//! - OC-β / Grok-γ rumors list to+cc, NO bcc.
//! - each bcc's rumor lists to+cc+itself but NOT the other bcc.
//! - non-recipients cannot unwrap (cryptographically enforced).
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use nostr_sdk::prelude::*;
use toron_core::nostr::gift_wrap;

/// `p`-tag roles in a rumor: ["p", npub, relay, role].
fn p_roles(rumor: &UnsignedEvent) -> Vec<(String, String)> {
    rumor
        .tags
        .iter()
        .filter(|t| t.as_slice().first().map(String::as_str) == Some("p"))
        .map(|t| {
            let cells = t.as_slice();
            let npub = cells.get(1).map(String::as_str).unwrap_or("").to_string();
            let role = cells.get(3).map(String::as_str).unwrap_or("to").to_string();
            (npub, role)
        })
        .collect()
}

#[tokio::test]
async fn sc3_bcc_privacy_matrix() {
    let sender = Keys::generate(); // Pi-α
    let to_keys = Keys::generate(); // OC-β
    let cc_keys = Keys::generate(); // Grok-γ
    let b1_keys = Keys::generate(); // Pi-δ
    let b2_keys = Keys::generate(); // Pi-ε
    let stranger_keys = Keys::generate();

    let to = to_keys.public_key();
    let cc = cc_keys.public_key();
    let bcc1 = b1_keys.public_key();
    let bcc2 = b2_keys.public_key();

    let npub = |pk: &PublicKey| pk.to_hex();

    // --- visible rumor: to + cc only, NO bcc (ADR G3) ---
    let visible = gift_wrap::build_rumor(
        sender.public_key(),
        Timestamp::from(1_700_000_000),
        14.into(),
        vec![
            Tag::parse(["p", &npub(&to), "", "to"]).unwrap(),
            Tag::parse(["p", &npub(&cc), "", "cc"]).unwrap(),
        ],
        "body",
    );

    // --- bcc-variant rumors: to + cc + ONE bcc (each omits the other) ---
    let variant1 = gift_wrap::build_rumor(
        sender.public_key(),
        Timestamp::from(1_700_000_000),
        14.into(),
        vec![
            Tag::parse(["p", &npub(&to), "", "to"]).unwrap(),
            Tag::parse(["p", &npub(&cc), "", "cc"]).unwrap(),
            Tag::parse(["p", &npub(&bcc1), "", "bcc"]).unwrap(),
        ],
        "body",
    );
    let variant2 = gift_wrap::build_rumor(
        sender.public_key(),
        Timestamp::from(1_700_000_000),
        14.into(),
        vec![
            Tag::parse(["p", &npub(&to), "", "to"]).unwrap(),
            Tag::parse(["p", &npub(&cc), "", "cc"]).unwrap(),
            Tag::parse(["p", &npub(&bcc2), "", "bcc"]).unwrap(),
        ],
        "body",
    );

    // --- wrap one copy per recipient ---
    let wrap_to = gift_wrap::gift_wrap(&sender, &to, visible.clone()).unwrap();
    let wrap_cc = gift_wrap::gift_wrap(&sender, &cc, visible).unwrap();
    let wrap_b1 = gift_wrap::gift_wrap(&sender, &bcc1, variant1).unwrap();
    let wrap_b2 = gift_wrap::gift_wrap(&sender, &bcc2, variant2).unwrap();

    // --- assertions ---

    // to + cc see the VISIBLE rumor: to+cc, no bcc.
    for (keys, wrap) in [(&to_keys, &wrap_to), (&cc_keys, &wrap_cc)] {
        let uw = gift_wrap::unwrap(keys, wrap).expect("recipient unwraps");
        let roles = p_roles(&uw.rumor);
        assert!(roles.iter().any(|(n, r)| n == &npub(&to) && r == "to"));
        assert!(roles.iter().any(|(n, r)| n == &npub(&cc) && r == "cc"));
        assert!(
            !roles.iter().any(|(_, r)| r == "bcc"),
            "visible rumor has no bcc"
        );
    }

    // bcc1 sees its variant: to+cc+itself, NOT bcc2.
    let uw_b1 = gift_wrap::unwrap(&b1_keys, &wrap_b1).expect("bcc1 unwraps");
    let roles1 = p_roles(&uw_b1.rumor);
    assert!(roles1.iter().any(|(n, r)| n == &npub(&to) && r == "to"));
    assert!(roles1.iter().any(|(n, r)| n == &npub(&cc) && r == "cc"));
    assert!(roles1.iter().any(|(n, r)| n == &npub(&bcc1) && r == "bcc"));
    assert!(
        !roles1.iter().any(|(n, _)| n == &npub(&bcc2)),
        "bcc1 cannot see bcc2"
    );

    // bcc2 symmetric.
    let uw_b2 = gift_wrap::unwrap(&b2_keys, &wrap_b2).expect("bcc2 unwraps");
    let roles2 = p_roles(&uw_b2.rumor);
    assert!(roles2.iter().any(|(n, r)| n == &npub(&bcc2) && r == "bcc"));
    assert!(
        !roles2.iter().any(|(n, _)| n == &npub(&bcc1)),
        "bcc2 cannot see bcc1"
    );

    // Non-recipients cannot unwrap (cryptographically enforced).
    for wrap in [&wrap_to, &wrap_cc, &wrap_b1, &wrap_b2] {
        assert!(
            gift_wrap::unwrap(&stranger_keys, wrap).is_err(),
            "stranger cannot unwrap"
        );
    }

    // Cross-check: bcc1's wrap is NOT the visible wrap (would leak).
    assert_ne!(wrap_b1.id, wrap_to.id, "bcc variant is a distinct event");
    assert_ne!(wrap_b2.id, wrap_to.id, "bcc variant is a distinct event");
}
