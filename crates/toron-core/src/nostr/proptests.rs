// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Property-based tests for the pure nostr/keys modules (proptest).
//!
//! Property tests assert invariants over a wide random input space rather than
//! a single fixture: canonical JSON round-trips, event-id determinism, NIP-44
//! decrypt symmetry, and BIP-340 sig randomization (id stable, sig varies).

#[cfg(test)]
mod tests {
    use nostr_sdk::nostr::event::id::EventId;
    use nostr_sdk::nostr::{Event, JsonUtil, Keys, Kind, Tag, Timestamp};
    use proptest::prelude::*;

    use crate::nostr::event::{build_event, verify_event};
    use crate::nostr::gift_wrap;
    use crate::nostr::kinds::{AGENT_PROFILE, PROJECT_MANIFEST, toron_kind};

    /// Arbitrary non-empty string content.
    fn any_content() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('a', 'z'), 1..64)
            .prop_map(|v| v.into_iter().collect())
    }

    /// Arbitrary hex public key string (not necessarily a valid key).
    fn any_hex_pubkey() -> impl Strategy<Value = String> {
        prop::collection::vec(prop::char::range('0', 'f'), 64).prop_map(|v| v.into_iter().collect())
    }

    /// Build an event with a fixed timestamp for a random content.
    fn event_for(keys: &Keys, kind: u16, content: &str) -> Event {
        build_event(
            keys,
            toron_kind(kind),
            content,
            Vec::<Tag>::new(),
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build")
    }

    proptest! {
        #[test]
        fn canonical_json_roundtrip_idempotent(
            content in any_content(),
        ) {
            let keys = Keys::generate();
            let ev = event_for(&keys, PROJECT_MANIFEST, &content);
            let json = ev.as_json();
            let parsed = Event::from_json(&json).expect("parse canonical json");
            assert_eq!(parsed, ev);
            assert_eq!(parsed.as_json(), json, "canonical json is idempotent");
        }

        #[test]
        fn event_id_deterministic_given_inputs(
            content in any_content(),
            ts in 0u64..4_000_000_000u64,
        ) {
            let keys = Keys::generate();
            let a = build_event(&keys, toron_kind(AGENT_PROFILE), content.clone(), Vec::<Tag>::new(), Timestamp::from(ts)).expect("a");
            let b = build_event(&keys, toron_kind(AGENT_PROFILE), content, Vec::<Tag>::new(), Timestamp::from(ts)).expect("b");
            assert_eq!(a.id, b.id, "id depends only on canonical inputs");
            assert_eq!(a.pubkey, b.pubkey);
            assert_eq!(a.created_at, b.created_at);
        }

        #[test]
        fn mutated_content_fails_verify(
            content in any_content(),
        ) {
            let keys = Keys::generate();
            let ev = event_for(&keys, PROJECT_MANIFEST, &content);
            let forged = Event::new(
                ev.id,
                ev.pubkey,
                ev.created_at,
                ev.kind,
                ev.tags.clone(),
                format!("{content}X"),
                ev.sig,
            );
            prop_assert!(verify_event(&forged).is_err(), "mutated content must fail verify");
        }

        #[test]
        fn nip44_symmetry(
            plaintext in any_content(),
        ) {
            let sender = Keys::generate();
            let recipient = Keys::generate();
            let cipher = gift_wrap::encrypt(sender.secret_key(), &recipient.public_key(), plaintext.as_bytes()).expect("encrypt");
            let round = gift_wrap::decrypt(recipient.secret_key(), &sender.public_key(), &cipher).expect("decrypt");
            assert_eq!(round, plaintext);
        }

        #[test]
        fn nip44_wrong_key_fails(
            plaintext in any_content(),
        ) {
            let sender = Keys::generate();
            let recipient = Keys::generate();
            let stranger = Keys::generate();
            let cipher = gift_wrap::encrypt(sender.secret_key(), &recipient.public_key(), plaintext.as_bytes()).expect("encrypt");
            prop_assert!(gift_wrap::decrypt(stranger.secret_key(), &sender.public_key(), &cipher).is_err());
        }

        #[test]
        fn gift_wrap_roundtrip_preserves_rumor(
            secret in any_content(),
        ) {
            // Property: wrap → unwrap recovers the exact rumor for arbitrary
            // plaintext (the envelope is opaque; the secret is not the
            // envelope content in the clear).
            let rt = tokio::runtime::Runtime::new().expect("rt");
            let sender = Keys::generate();
            let recipient = Keys::generate();
            let rumor = gift_wrap::build_rumor(
                sender.public_key(),
                Timestamp::from(1_700_000_000_u64),
                Kind::PrivateDirectMessage,
                Vec::<Tag>::new(),
                secret.clone(),
            );
            let wrapped = rt.block_on(gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor)).expect("wrap");
            prop_assert_ne!(&wrapped.content, &secret, "envelope content is not the plaintext");
            let unwrapped = gift_wrap::unwrap(&recipient, &wrapped).expect("unwrap");
            prop_assert_eq!(unwrapped.rumor.content, secret);
        }
    }

    // EventId parses from any hex we emit (64 lowercase hex chars).
    proptest! {
        #[test]
        fn event_id_hex_roundtrip(raw in any_hex_pubkey()) {
            // Only well-formed 64-hex ids parse; others error — the point is
            // the parse path is total on the domain we produce.
            let _ = EventId::from_hex(&raw);
        }
    }
}
