// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Event build/sign/verify wrappers (pure, no I/O).
//!
//! Every bus publish goes through [`build_event`]. Events are built via the
//! rust-nostr canonical serialization so wire bytes are identical to a real
//! relay's expectations. Verification rejects forged/mutated events (wrong id
//! or sig).

use nostr_sdk::nostr::event::builder::Error as BuilderError;
use nostr_sdk::nostr::event::Error as EventError;
use nostr_sdk::nostr::{Event, EventBuilder, JsonUtil, Keys, Kind, Tag, Timestamp};

/// Error returned by verification.
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("event id is not canonical (mutated content/tags?)")]
    BadId,
    #[error("signature does not verify against pubkey")]
    BadSignature,
    #[error("event failed full verification: {0}")]
    Invalid(#[from] EventError),
}

/// Build and sign an event with an explicit `created_at` (deterministic).
///
/// `created_at` is unix seconds. For tests and ordering-sensitive callers pass
/// a fixed timestamp; for production pass [`Timestamp::now()`] (or the rumor
/// timestamp per ADR-0003 G7).
pub fn build_event(
    keys: &Keys,
    kind: Kind,
    content: impl Into<String>,
    tags: impl IntoIterator<Item = Tag>,
    created_at: Timestamp,
) -> Result<Event, BuilderError> {
    EventBuilder::new(kind, content)
        .tags(tags)
        .custom_created_at(created_at)
        .sign_with_keys(keys)
}

/// Verify a single event's canonical id and Schnorr signature.
///
/// Returns [`VerifyError::BadId`] when the id is not the sha256 of the
/// canonical serialization (forged/mutated content or tags), and
/// [`VerifyError::BadSignature`] when the signature does not check against the
/// event pubkey.
pub fn verify_event(event: &Event) -> Result<(), VerifyError> {
    if !event.verify_id() {
        return Err(VerifyError::BadId);
    }
    if !event.verify_signature() {
        return Err(VerifyError::BadSignature);
    }
    Ok(())
}

/// Verify an event against an expected author, rejecting any other pubkey.
pub fn verify_event_author(event: &Event, expected: &nostr_sdk::nostr::PublicKey) -> Result<(), VerifyError> {
    verify_event(event)?;
    if &event.pubkey != expected {
        return Err(VerifyError::BadSignature);
    }
    Ok(())
}

/// Wire-identical serialization — the exact bytes a relay would store.
pub fn canonical_json(event: &Event) -> String {
    event.as_json()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::nostr::{SecretKey, Tag};

    // Fixed test keypair: secret key 0x01 → known pubkey (NIP-19-style hex).
    fn test_keys() -> Keys {
        let sk = SecretKey::from_hex(
            "0000000000000000000000000000000000000000000000000000000000000001",
        )
        .expect("valid secret key");
        Keys::new(sk)
    }

    fn sample_tags() -> Vec<Tag> {
        vec![
            Tag::parse(["t", "demo"]).expect("valid tag"),
            Tag::parse(["p", "f86c44a2de95d9149b51c6a29afeabba264c18e2fa7c49de93424a0c56947785"])
                .expect("valid tag"),
        ]
    }

    #[test]
    fn test_golden_known_keypair_known_event_id() {
        let keys = test_keys();
        let ts = Timestamp::from(1_700_000_000_u64);
        let event = build_event(
            &keys,
            Kind::PrivateDirectMessage,
            "hello world",
            sample_tags(),
            ts,
        )
        .expect("build ok");

        // Golden: fixed keypair (sk=0x01) + fixed created_at + fixed tags →
        // fixed canonical event id. Captured from an independent build.
        assert_eq!(
            event.id.to_hex(),
            "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b"
        );
        assert_eq!(
            event.pubkey.to_hex(),
            "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
        );
        // Canonical JSON must round-trip byte-identical (wire-compatible).
        let golden_json = r#"{"id":"b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b","pubkey":"79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798","created_at":1700000000,"kind":14,"tags":[["t","demo"],["p","f86c44a2de95d9149b51c6a29afeabba264c18e2fa7c49de93424a0c56947785"]],"content":"hello world""#;
        // id + pubkey + created_at + kind + tags + content are the golden
        // prefix; sig is BIP-340-randomized so strip it before comparing.
        let json = event.as_json();
        let (prefix, _sig) = json.split_at(json.rfind(r#","sig":"#).expect("sig field"));
        assert_eq!(prefix, golden_json);
    }

    #[test]
    fn test_golden_deterministic_id_across_builds() {
        let keys = test_keys();
        let ts = Timestamp::from(1_700_000_000_u64);
        let event = build_event(
            &keys,
            Kind::PrivateDirectMessage,
            "hello world",
            sample_tags(),
            ts,
        )
        .expect("build ok");
        let event2 = build_event(
            &keys,
            Kind::PrivateDirectMessage,
            "hello world",
            sample_tags(),
            ts,
        )
        .expect("build ok");
        // Same inputs → same canonical id (sig is randomized by BIP-340 aux-rand).
        assert_eq!(event.id, event2.id);
        assert_eq!(event.as_json()[..event.as_json().rfind(r#","sig":"#).unwrap()],
                   event2.as_json()[..event2.as_json().rfind(r#","sig":"#).unwrap()]);
    }

    #[test]
    fn test_golden_expected_pubkey() {
        let keys = test_keys();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "canonical serialization test",
            Vec::<Tag>::new(),
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build ok");

        // pubkey for secret key 0x01 is a fixed, well-known value.
        assert_eq!(
            event.pubkey.to_hex(),
            "79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
        );
    }

    #[test]
    fn test_verify_accepts_valid_event() {
        let keys = test_keys();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "valid event",
            sample_tags(),
            Timestamp::now(),
        )
        .expect("build ok");
        verify_event(&event).expect("valid event verifies");
        verify_event_author(&event, &keys.public_key()).expect("author matches");
    }

    #[test]
    fn test_mutate_content_verify_fails() {
        let keys = test_keys();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "original content",
            sample_tags(),
            Timestamp::now(),
        )
        .expect("build ok");

        // Mutate content but keep id+sig → id no longer canonical.
        let forged = Event::new(
            event.id,
            event.pubkey,
            event.created_at,
            event.kind,
            event.tags.clone(),
            "MUTATED CONTENT",
            event.sig,
        );
        match verify_event(&forged) {
            Err(VerifyError::BadId) => {}
            other => panic!("expected BadId, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn test_mutate_sig_verify_fails() {
        let keys = test_keys();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "content",
            sample_tags(),
            Timestamp::now(),
        )
        .expect("build ok");

        // Flip a byte in the signature (keep id + rest intact).
        let mut sig_bytes = event.sig.serialize();
        sig_bytes[0] ^= 0xFF;
        let bad_sig = nostr_sdk::secp256k1::schnorr::Signature::from_slice(&sig_bytes)
            .expect("sig parse");

        let forged = Event::new(
            event.id,
            event.pubkey,
            event.created_at,
            event.kind,
            event.tags.clone(),
            event.content.clone(),
            bad_sig,
        );
        match verify_event(&forged) {
            Err(VerifyError::BadSignature) => {}
            other => panic!("expected BadSignature, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn test_mutate_tags_verify_fails() {
        let keys = test_keys();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "content",
            sample_tags(),
            Timestamp::now(),
        )
        .expect("build ok");

        // Add a tag → id no longer canonical.
        let mut tags = event.tags.clone();
        tags.push(Tag::parse(["t", "forged"]).expect("tag"));
        let forged = Event::new(
            event.id,
            event.pubkey,
            event.created_at,
            event.kind,
            tags,
            event.content.clone(),
            event.sig,
        );
        match verify_event(&forged) {
            Err(VerifyError::BadId) => {}
            other => panic!("expected BadId, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn test_canonical_json_roundtrip() {
        let keys = test_keys();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "roundtrip",
            sample_tags(),
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build ok");

        let json = canonical_json(&event);
        let parsed = Event::from_json(json).expect("parse canonical json");
        assert_eq!(parsed, event);
    }

    #[test]
    fn test_author_mismatch_rejected() {
        let keys = test_keys();
        let other = Keys::generate();
        let event = build_event(
            &keys,
            Kind::TextNote,
            "signed by test_keys",
            Vec::<Tag>::new(),
            Timestamp::now(),
        )
        .expect("build ok");
        let err = verify_event_author(&event, &other.public_key()).expect_err("author mismatch");
        assert!(matches!(err, VerifyError::BadSignature));
    }
}
