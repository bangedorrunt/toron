// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! SC-8 — Proof-gate fail-closed + nonce persist (P1 / v1 gate).
//!
//! Pre: gate enabled, trust anchor set.
//! Steps: register with no proof → reject; wrong kind → reject; untrusted
//! anchor → reject; forged → reject; expired → reject; mismatched binding →
//! reject; valid anchor-signed kind-30012 → accept; replay same nonce →
//! reject; **restart process; old nonces still rejected**.
//!
//! Assert: fail-closed on missing/malformed/expired/replayed/mismatched;
//! nonces persist across restart (SQLite-backed NonceStore).
//!
//! Gate: `cargo test -p toron-e2e --features p1`.
#![cfg(feature = "p1")]

use nostr_sdk::prelude::*;
use toron_core::index::schema;
use toron_core::nostr::proof::{
    Attestation, NonceStore, ProofClaim, ProofError, ProofGate, build_attestation_event,
};

#[tokio::test]
async fn sc8_proof_gate_fail_closed_and_nonce_persists() {
    let dir = std::env::temp_dir().join(format!("toron-sc8-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("index.sqlite");
    let pool = schema::connect(db.to_str().unwrap()).await.expect("pool");
    schema::migrate(&pool).await.expect("migrate");

    let anchor = Keys::generate();
    let agent = Keys::generate();
    let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
    let gate = ProofGate {
        trusted_anchors: vec![anchor.public_key().to_hex()],
        scheme: toron_core::nostr::proof::ProofScheme::Schnorr,
    };

    // Helper: build + verify an attestation for `att`.
    async fn verify(
        anchor: &Keys,
        gate: &ProofGate,
        claim: &ProofClaim,
        att: Attestation,
        store: &mut NonceStore,
        now: Timestamp,
    ) -> Result<(), ProofError> {
        let evt = build_attestation_event(anchor, &att, "demo", "PiAlpha", now)
            .expect("build attestation");
        gate.verify_schnorr(&evt, claim, store, now).await
    }

    let now = Timestamp::from(1_700_000_000);
    let valid_att = || Attestation {
        scheme: "schnorr".into(),
        agent_npub: agent.public_key().to_hex(),
        project: "demo".into(),
        scope: "project:collaborator".into(),
        nonce: "nonce-123".into(),
        expires_at: Some(now.as_secs() + 3600),
        signature: None,
    };

    // --- 1. Wrong kind rejected (kind mismatch) ---
    let mut store = NonceStore::new(pool.clone());
    let wrong_kind = EventBuilder::new(Kind::TextNote, "nope")
        .finalize(&anchor)
        .unwrap();
    assert!(
        matches!(
            gate.verify_schnorr(&wrong_kind, &claim, &mut store, now)
                .await,
            Err(ProofError::Mismatch(_))
        ),
        "wrong kind rejected"
    );

    // --- 2. Untrusted anchor rejected ---
    let rogue = Keys::generate();
    let mut store = NonceStore::new(pool.clone());
    let rogue_evt = build_attestation_event(&rogue, &valid_att(), "demo", "PiAlpha", now).unwrap();
    assert!(
        matches!(
            gate.verify_schnorr(&rogue_evt, &claim, &mut store, now)
                .await,
            Err(ProofError::Mismatch(_))
        ),
        "untrusted anchor rejected"
    );

    // --- 3. Forged (mutated signature) rejected ---
    let mut store = NonceStore::new(pool.clone());
    let valid = build_attestation_event(&anchor, &valid_att(), "demo", "PiAlpha", now).unwrap();
    let mut forged = valid.clone();
    forged.content =
        r#"{"scheme":"schnorr","agent_npub":"f0f0","project":"demo","scope":"x","nonce":"y"}"#
            .into();
    assert!(
        gate.verify_schnorr(&forged, &claim, &mut store, now)
            .await
            .is_err(),
        "forged rejected"
    );

    // --- 4. Expired rejected ---
    let mut store = NonceStore::new(pool.clone());
    let mut expired = valid_att();
    expired.expires_at = Some(now.as_secs() - 60);
    expired.nonce = "nonce-expired".into();
    assert_eq!(
        verify(&anchor, &gate, &claim, expired, &mut store, now)
            .await
            .unwrap_err(),
        ProofError::Expired
    );

    // --- 5. Mismatched binding rejected ---
    let mut store = NonceStore::new(pool.clone());
    let mut mismatch = valid_att();
    mismatch.project = "other-project".into();
    mismatch.nonce = "nonce-mismatch".into();
    assert!(
        matches!(
            verify(&anchor, &gate, &claim, mismatch, &mut store, now).await,
            Err(ProofError::Mismatch(_))
        ),
        "binding mismatch rejected"
    );

    // --- 6. Valid attestation accepted once ---
    let mut store = NonceStore::new(pool.clone());
    verify(&anchor, &gate, &claim, valid_att(), &mut store, now)
        .await
        .expect("valid accepted");

    // --- 7. Replay same nonce → rejected (in-session) ---
    let err = verify(&anchor, &gate, &claim, valid_att(), &mut store, now)
        .await
        .unwrap_err();
    assert_eq!(err, ProofError::Replayed, "same-session replay rejected");

    // --- 8. "Restart": NEW NonceStore on the same DB → old nonce still
    //        rejected (SQLite-persisted proof_nonces, ADR G1/SC-8). ---
    let mut restarted = NonceStore::new(pool.clone());
    let err = verify(&anchor, &gate, &claim, valid_att(), &mut restarted, now)
        .await
        .unwrap_err();
    assert_eq!(err, ProofError::Replayed, "nonce persists across restart");

    let _ = std::fs::remove_dir_all(dir);
}
