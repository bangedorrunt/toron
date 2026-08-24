// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! NIP-44 + NIP-59 gift-wrap primitives — the NIP-17 building blocks.
//!
//! Pure (no I/O). `gift_wrap` produces the wire-identical kind-1059 envelope
//! (seal → wrap) that a real relay stores; `unwrap` recovers the rumor for the
//! recipient. Per ADR-0003 G7, NIP-17 randomizes `created_at` on the seal AND
//! the wrap (up to 2 days back) — the envelope timestamp is NOT the mail
//! timeline; the rumor's `created_at` is.

use crate::nostr::UnsignedEvent;
use crate::nostr::{Event, Keys, Kind, PublicKey, SecretKey, Tag, Timestamp};
use ::nostr::error::Error as BuilderError;
use ::nostr::event::FinalizeEvent;
use ::nostr::nips::nip44;
use ::nostr::nips::nip59::GiftWrapBuilder;

/// Error returned by the gift-wrap/unwrap path.
#[derive(Debug, thiserror::Error)]
pub enum GiftWrapError {
    #[error("not a gift wrap event (kind must be 1059)")]
    NotGiftWrap,
    #[error("seal sender does not match rumor author")]
    SenderMismatch,
    #[error("nostr crypto/json failure: {0}")]
    Nostr(#[from] ::nostr::error::Error),
    #[error("event verification failure: {0}")]
    Verify(#[from] crate::nostr::event::VerifyError),
}

/// Unwrapped gift: the sender's pubkey (from the seal) + the unsigned rumor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnwrappedGift {
    /// Sender public key (seal signer).
    pub sender: PublicKey,
    /// The unsigned rumor (kind 14/15 etc.).
    pub rumor: UnsignedEvent,
}

/// NIP-44 encrypt between two keys.
pub fn encrypt(
    secret_key: &SecretKey,
    recipient: &PublicKey,
    content: impl AsRef<[u8]>,
) -> Result<String, ::nostr::error::Error> {
    nip44::encrypt(secret_key, recipient, content, nip44::Version::V2)
}

/// NIP-44 decrypt between two keys.
pub fn decrypt(
    secret_key: &SecretKey,
    sender: &PublicKey,
    payload: impl AsRef<[u8]>,
) -> Result<String, ::nostr::error::Error> {
    nip44::decrypt(secret_key, sender, payload)
}

/// Build an unsigned rumor (the mail timeline anchor; see G7).
pub fn build_rumor(
    author: PublicKey,
    created_at: Timestamp,
    kind: Kind,
    tags: impl IntoIterator<Item = Tag>,
    content: impl Into<String>,
) -> UnsignedEvent {
    UnsignedEvent::new(author, created_at, kind, tags, content)
}

/// Wrap a rumor for one recipient: kind-13 seal (sender-signed, content
/// NIP-44-encrypted to recipient) → kind-1059 gift wrap signed by a random
/// one-time key. Both seal and wrap get a randomized `created_at` (NIP-17).
///
/// rust-nostr 0.45 `GiftWrapBuilder` is synchronous (os-rng).
pub fn gift_wrap(
    sender: &Keys,
    recipient: &PublicKey,
    rumor: UnsignedEvent,
) -> Result<Event, BuilderError> {
    GiftWrapBuilder::new(*recipient, rumor).finalize(sender)
}

/// Unwrap a gift wrap for the recipient key.
///
/// Mirrors `UnwrappedGift::from_gift_wrap` but synchronous (pure, no signer
/// trait indirection). Verifies the seal's event id + signature, decrypts the
/// rumor, and enforces seal-pubkey == rumor-pubkey (NIP-59 SenderMismatch).
pub fn unwrap(recipient: &Keys, gift_wrap: &Event) -> Result<UnwrappedGift, GiftWrapError> {
    if gift_wrap.kind != Kind::GiftWrap {
        return Err(GiftWrapError::NotGiftWrap);
    }
    // NIP-44: verify the envelope Schnorr sig before decrypting ciphertext.
    crate::nostr::event::verify_event(gift_wrap)?;

    // Outer: decrypt seal with recipient key against the wrap's random pubkey.
    let seal_json = decrypt(
        recipient.secret_key(),
        &gift_wrap.pubkey,
        &gift_wrap.content,
    )?;
    let seal = Event::from_json(seal_json)?;
    crate::nostr::event::verify_event(&seal)?;
    if seal.kind != Kind::Seal {
        return Err(GiftWrapError::NotGiftWrap);
    }

    // Inner: decrypt rumor with recipient key against the seal's sender pubkey.
    let rumor_json = decrypt(recipient.secret_key(), &seal.pubkey, &seal.content)?;
    let rumor = UnsignedEvent::from_json(rumor_json)?;

    // NIP-59: the rumor author must equal the seal signer.
    if rumor.pubkey != seal.pubkey {
        return Err(GiftWrapError::SenderMismatch);
    }

    Ok(UnwrappedGift {
        sender: seal.pubkey,
        rumor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::JOB_RESULT;

    fn sender_keys() -> Keys {
        Keys::parse("6b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e")
            .expect("valid key")
    }

    fn recipient_keys() -> Keys {
        Keys::parse("7b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e")
            .expect("valid key")
    }

    fn stranger_keys() -> Keys {
        Keys::parse("5b911fd37cdf5c81d4c0adb1ab7fa822ed253ab0ad9aa18d77257c88b29b718e")
            .expect("valid key")
    }

    fn sample_rumor(author: PublicKey) -> UnsignedEvent {
        build_rumor(
            author,
            Timestamp::from(1_700_000_000_u64),
            Kind::PrivateDirectMessage,
            [
                Tag::public_key(author),
                Tag::parse(["subject", "hello"]).expect("subject tag"),
            ],
            "secret payload",
        )
    }

    #[test]
    fn test_nip44_roundtrip() {
        let sender = sender_keys();
        let recipient = recipient_keys();
        let cipher =
            encrypt(sender.secret_key(), &recipient.public_key(), "top secret").expect("encrypt");
        assert_ne!(cipher, "top secret");
        let plain =
            decrypt(recipient.secret_key(), &sender.public_key(), &cipher).expect("decrypt");
        assert_eq!(plain, "top secret");
    }

    #[test]
    fn test_nip44_wrong_recipient_cannot_decrypt() {
        let sender = sender_keys();
        let recipient = recipient_keys();
        let stranger = stranger_keys();
        let cipher =
            encrypt(sender.secret_key(), &recipient.public_key(), "secret").expect("encrypt");
        assert!(decrypt(stranger.secret_key(), &sender.public_key(), &cipher).is_err());
    }

    #[test]
    fn test_gift_wrap_roundtrip_rumor_identical() {
        let sender = sender_keys();
        let recipient = recipient_keys();
        let rumor = sample_rumor(sender.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor.clone()).expect("wrap");

        assert_eq!(wrapped.kind, Kind::GiftWrap);
        assert_eq!(wrapped.kind.as_u16(), 1059);

        let unwrapped = unwrap(&recipient, &wrapped).expect("unwrap");
        assert_eq!(unwrapped.sender, sender.public_key());
        assert_eq!(unwrapped.rumor.pubkey, rumor.pubkey);
        assert_eq!(unwrapped.rumor.kind, Kind::PrivateDirectMessage);
        assert_eq!(unwrapped.rumor.content, "secret payload");
        // Rumor tags survive the wrap.
        assert_eq!(unwrapped.rumor.tags, rumor.tags);
    }

    #[test]
    fn test_wrong_recipient_cannot_unwrap() {
        let sender = sender_keys();
        let recipient = recipient_keys();
        let stranger = stranger_keys();
        let rumor = sample_rumor(sender.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        assert!(unwrap(&stranger, &wrapped).is_err());
    }

    #[test]
    fn test_non_recipient_cannot_read_seal() {
        // The seal content is NIP-44-encrypted to the recipient; a third party
        // who observes the wrap cannot decrypt the seal even with the sender's
        // pubkey. The wrap's random key never appears to outsiders.
        let sender = sender_keys();
        let recipient = recipient_keys();
        let stranger = stranger_keys();
        let rumor = sample_rumor(sender.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        // Stranger attempts: cannot decrypt the seal payload.
        let seal_json = decrypt(stranger.secret_key(), &wrapped.pubkey, &wrapped.content);
        assert!(seal_json.is_err());
    }

    #[test]
    fn test_sender_mismatch_rejected() {
        // A rumor that lies about its author must be rejected (NIP-59).
        let sender = sender_keys();
        let recipient = recipient_keys();
        let impersonated = stranger_keys();
        let rumor = sample_rumor(impersonated.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        match unwrap(&recipient, &wrapped) {
            Err(GiftWrapError::SenderMismatch) => {}
            other => panic!("expected SenderMismatch, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn test_non_gift_wrap_rejected() {
        let sender = sender_keys();
        let not_a_wrap = build_event(
            &sender,
            Kind::TextNote,
            "plain note",
            Vec::<Tag>::new(),
            Timestamp::now(),
        )
        .expect("build");
        match unwrap(&recipient_keys(), &not_a_wrap) {
            Err(GiftWrapError::NotGiftWrap) => {}
            other => panic!("expected NotGiftWrap, got {:?}", other.map(|_| ())),
        }
    }

    #[test]
    fn test_wrap_randomizes_created_at() {
        // NIP-17: seal + wrap created_at are randomized (up to 2 days back).
        let sender = sender_keys();
        let recipient = recipient_keys();
        let rumor = sample_rumor(sender.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        // The rumor's created_at (mail timeline) differs from the wrap's.
        assert_ne!(wrapped.created_at.as_secs(), 1_700_000_000_u64);
        let now = Timestamp::now().as_secs();
        assert!(wrapped.created_at.as_secs() <= now);
        assert!(wrapped.created_at.as_secs() + 172_800 >= now);
    }

    #[test]
    fn test_build_rumor_sets_author_and_timeline() {
        let sender = sender_keys();
        let rumor = sample_rumor(sender.public_key());
        assert_eq!(rumor.pubkey, sender.public_key());
        assert_eq!(rumor.created_at.as_secs(), 1_700_000_000_u64);
        assert_eq!(rumor.kind.as_u16(), 14);
        assert_eq!(rumor.kind, Kind::PrivateDirectMessage);
    }

    #[test]
    fn test_wrap_uses_fresh_one_time_key_per_wrap() {
        // NIP-59 metadata hiding: the wrap's pubkey MUST NOT be the sender's
        // identity key (a relay observer would learn the sender), and two
        // wraps from the same sender MUST use different one-time keys
        // (unlinkability — adr-0003 "one-time random key per wrap" claim).
        let sender = sender_keys();
        let recipient = recipient_keys();
        let rumor = sample_rumor(sender.public_key());
        let w1 = gift_wrap(&sender, &recipient.public_key(), rumor.clone()).expect("wrap1");
        let w2 = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap2");

        assert_ne!(
            w1.pubkey,
            sender.public_key(),
            "wrap pubkey must not leak the sender identity"
        );
        assert_ne!(
            w2.pubkey,
            sender.public_key(),
            "wrap pubkey must not leak the sender identity"
        );
        assert_ne!(
            w1.pubkey, w2.pubkey,
            "each wrap uses a FRESH one-time key (unlinkable wraps)"
        );
        // The seal inside is sender-signed; unwrap recovers the true sender.
        let u = unwrap(&recipient, &w1).expect("unwrap");
        assert_eq!(u.sender, sender.public_key());
    }

    #[test]
    fn test_tampered_wrap_envelope_rejected_before_decrypt() {
        let sender = sender_keys();
        let recipient = recipient_keys();
        let rumor = sample_rumor(sender.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        let mut sig_bytes = wrapped.sig.to_bytes();
        sig_bytes[0] ^= 0xFF;
        let bad_sig = crate::nostr::Signature::from_slice(&sig_bytes).expect("sig parse");
        let forged = Event::new(
            wrapped.id,
            wrapped.pubkey,
            wrapped.created_at,
            wrapped.kind,
            wrapped.tags.clone(),
            wrapped.content.clone(),
            bad_sig,
        );
        match unwrap(&recipient, &forged) {
            Err(GiftWrapError::Verify(_)) => {}
            other => panic!(
                "forged wrap sig must fail verify before decrypt, got {:?}",
                other.map(|_| ())
            ),
        }
    }

    #[test]
    fn test_tampered_seal_rejected() {
        // The seal (kind-13) is sender-SIGNED, not just encrypted. Tampering
        // the seal's content invalidates its id/sig; unwrap must reject it at
        // verify_event (gift_wrap.rs:100), never deliver a forged rumor.
        let sender = sender_keys();
        let recipient = recipient_keys();
        let rumor = sample_rumor(sender.public_key());

        // Build the honest seal, then tamper with its content.
        let seal =
            ::nostr::nips::nip59::GiftWrapSealBuilder::new(rumor.clone(), recipient.public_key())
                .finalize(&sender)
                .expect("seal");
        let tampered = Event::new(
            seal.id,
            seal.pubkey,
            seal.created_at,
            seal.kind,
            seal.tags.clone(),
            "TAMPERED",
            seal.sig,
        );
        // Wrap the tampered seal as a valid-looking gift wrap (ephemeral key).
        let ephemeral = Keys::generate();
        let content = encrypt(
            ephemeral.secret_key(),
            &recipient.public_key(),
            tampered.as_json(),
        )
        .expect("encrypt tampered seal");
        let wrapped = crate::nostr::EventBuilder::new(Kind::GiftWrap, content)
            .tag(Tag::public_key(recipient.public_key()))
            .finalize(&ephemeral)
            .expect("wrap tampered seal");
        match unwrap(&recipient, &wrapped) {
            Err(GiftWrapError::Verify(_)) => {}
            other => panic!(
                "tampered seal must fail verify, got {:?}",
                other.map(|_| ())
            ),
        }
    }

    #[test]
    fn test_rumor_content_not_visible_in_wrap() {
        let sender = sender_keys();
        let recipient = recipient_keys();
        let rumor = sample_rumor(sender.public_key());
        let wrapped = gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");
        assert!(!wrapped.content.contains("secret payload"));
        // Job kinds work through the same wrapper.
        let job_rumor = build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000_u64),
            crate::nostr::kinds::toron_kind(JOB_RESULT),
            Vec::<Tag>::new(),
            "job done",
        );
        let wrapped_job =
            gift_wrap(&sender, &recipient.public_key(), job_rumor.clone()).expect("wrap job");
        let unwrapped = unwrap(&recipient, &wrapped_job).expect("unwrap job");
        assert_eq!(unwrapped.rumor.kind.as_u16(), JOB_RESULT);
    }
}
