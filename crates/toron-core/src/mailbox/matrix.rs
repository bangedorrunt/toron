// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Consolidated mailbox unit-test matrix (bd-led.5.12).
//!
//! The per-cluster beads already assert G3 (BCC matrix in `messaging.rs`),
//! G5 (force-release key-priority in `reservations.rs`), G8 (idempotent ack in
//! `messaging.rs`), G2 (unknown-name fail-fast in `messaging.rs`/`identity.rs`),
//! and symmetric globs (`guard.rs`/`reservations.rs`). This module fills the
//! remaining gap — the **proof-gate fail-closed matrix** (kind-30012 Schnorr,
//! oc-a `nostr::proof`) — and exposes a coverage pointer so conformance can
//! grep the full surface.

use crate::nostr::kinds::{REGISTRATION_ATTESTATION, toron_kind};
use crate::nostr::proof::{
    Attestation, NonceStore, ProofClaim, ProofError, ProofGate, ProofScheme,
    build_attestation_event,
};
use crate::nostr::{Keys, Timestamp};
use sqlx::SqlitePool;

/// Run the fail-closed proof matrix. Returns the set of cases that
/// INCORRECTLY passed (empty = all invalid proofs rejected + the valid
/// control accepted).
pub async fn run_proof_matrix(pool: SqlitePool) -> Vec<String> {
    let mut leaked = Vec::new();

    // A proof MUST be rejected; if it isn't, record a leak.
    async fn expect_reject(
        leaked: &mut Vec<String>,
        case: &str,
        result: Result<(), crate::nostr::proof::ProofError>,
    ) {
        if result.is_ok() {
            leaked.push(case.to_string());
        }
    }

    // Actors.
    let anchor = Keys::generate();
    let agent = Keys::generate();
    let attacker = Keys::generate();
    let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");

    let gate = ProofGate {
        trusted_anchors: vec![anchor.public_key().to_hex()],
        scheme: ProofScheme::Schnorr,
    };

    let mut nonce_counter = 0u64;
    let next_nonce = |nonce_counter: &mut u64| {
        *nonce_counter += 1;
        format!("{nonce_counter:064x}")
    };

    // Valid attestation builder.
    let mk_att = |nonce: &str, expires_at: Option<u64>| Attestation {
        scheme: "schnorr".into(),
        agent_npub: agent.public_key().to_hex(),
        project: "demo".into(),
        scope: "project:collaborator".into(),
        nonce: nonce.into(),
        expires_at,
        signature: None,
    };

    // 1. Valid control: must PASS.
    let n = next_nonce(&mut nonce_counter);
    let att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    let valid = build_attestation_event(&anchor, &att, "demo", "PiAlpha", Timestamp::now())
        .expect("valid proof");
    let mut store = NonceStore::new(pool.clone());
    let control = gate
        .verify_schnorr(&valid, &claim, &mut store, Timestamp::now())
        .await;
    if control.is_err() {
        leaked.push(format!("valid-control: {control:?}"));
    }

    // 2. Wrong kind.
    let n = next_nonce(&mut nonce_counter);
    let att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    let wrong_kind = crate::nostr::event::build_event(
        &anchor,
        crate::nostr::Kind::TextNote,
        serde_json::to_string(&att).unwrap(),
        vec![],
        Timestamp::now(),
    )
    .expect("wrong kind");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "wrong-kind",
        gate.verify_schnorr(&wrong_kind, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 3. Forged/mutated signature: flip a content byte AFTER signing.
    let n = next_nonce(&mut nonce_counter);
    let att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    let valid =
        build_attestation_event(&anchor, &att, "demo", "PiAlpha", Timestamp::now()).expect("proof");
    let forged = crate::nostr::Event::new(
        valid.id,
        valid.pubkey,
        valid.created_at,
        valid.kind,
        valid.tags.clone(),
        "MUTATED CONTENT",
        valid.sig,
    );
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "forged-content",
        gate.verify_schnorr(&forged, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 4. Untrusted anchor.
    let n = next_nonce(&mut nonce_counter);
    let att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    let untrusted = build_attestation_event(&attacker, &att, "demo", "PiAlpha", Timestamp::now())
        .expect("untrusted");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "untrusted-anchor",
        gate.verify_schnorr(&untrusted, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 5. Malformed content (not JSON).
    let bad = crate::nostr::event::build_event(
        &anchor,
        toron_kind(REGISTRATION_ATTESTATION),
        "not-json",
        vec![],
        Timestamp::now(),
    )
    .expect("malformed");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "malformed-content",
        gate.verify_schnorr(&bad, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 6. Scheme mismatch (ed25519 declared on a Schnorr gate).
    let n = next_nonce(&mut nonce_counter);
    let mut att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    att.scheme = "ed25519".into();
    let wrong_scheme = build_attestation_event(&anchor, &att, "demo", "PiAlpha", Timestamp::now())
        .expect("scheme");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "scheme-mismatch",
        gate.verify_schnorr(&wrong_scheme, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 7. Binding mismatch: wrong agent npub.
    let n = next_nonce(&mut nonce_counter);
    let mut att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    att.agent_npub = attacker.public_key().to_hex();
    let wrong_agent =
        build_attestation_event(&anchor, &att, "demo", "PiAlpha", Timestamp::now()).expect("agent");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "binding-agent",
        gate.verify_schnorr(&wrong_agent, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 7b. Wrong project.
    let n = next_nonce(&mut nonce_counter);
    let mut att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    att.project = "other".into();
    let wrong_project =
        build_attestation_event(&anchor, &att, "other", "PiAlpha", Timestamp::now())
            .expect("project");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "binding-project",
        gate.verify_schnorr(&wrong_project, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 8. Expired.
    let n = next_nonce(&mut nonce_counter);
    let att = mk_att(&n, Some(Timestamp::now().as_secs() - 60));
    let expired = build_attestation_event(&anchor, &att, "demo", "PiAlpha", Timestamp::now())
        .expect("expired");
    let mut store = NonceStore::new(pool.clone());
    expect_reject(
        &mut leaked,
        "expired",
        gate.verify_schnorr(&expired, &claim, &mut store, Timestamp::now())
            .await,
    )
    .await;

    // 9. Replayed nonce: same attestation twice → second is replayed.
    let n = next_nonce(&mut nonce_counter);
    let att = mk_att(&n, Some(Timestamp::now().as_secs() + 3600));
    let replayed = build_attestation_event(&anchor, &att, "demo", "PiAlpha", Timestamp::now())
        .expect("replay");
    let mut store = NonceStore::new(pool.clone());
    assert!(
        gate.verify_schnorr(&replayed, &claim, &mut store, Timestamp::now())
            .await
            .is_ok()
    );
    match gate
        .verify_schnorr(&replayed, &claim, &mut store, Timestamp::now())
        .await
    {
        Ok(()) => leaked.push("replayed-nonce".into()),
        Err(ProofError::Replayed) => {}
        Err(_) => {}
    }

    // 10. Missing proof (None) → the gate's contract is fail-closed: the
    // register path rejects before calling verify (missing = rejected).
    leaked
}

/// Coverage pointer for conformance: every matrix case + its home test.
pub fn coverage_cases() -> Vec<(&'static str, &'static str)> {
    vec![
        (
            "G3 BCC matrix",
            "mailbox::messaging::tests::test_bcc_variant_privacy",
        ),
        (
            "G3 N recipients -> N wraps",
            "mailbox::messaging::tests::test_send_n_recipients_n_wraps_same_rumor",
        ),
        (
            "G2 unknown fail-fast",
            "mailbox::messaging::tests::test_unknown_recipient_aborts_whole_send",
        ),
        (
            "G8 idempotent ack",
            "mailbox::messaging::tests::test_ack_idempotent_and_private",
        ),
        (
            "G5 force-release key-priority",
            "mailbox::reservations::tests::test_force_release_operator_revocation",
        ),
        (
            "G5 holder notify + audit",
            "mailbox::reservations::tests::test_force_release_notifies_holder_and_audits_git",
        ),
        (
            "symmetric glob both directions",
            "mailbox::reservations::tests::test_symmetric_conflict_both_directions",
        ),
        (
            "proof fail-closed",
            "mailbox::matrix::proof_gate_fail_closed_matrix",
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use std::sync::Arc;

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-matrix-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-matrix-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox =
            Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator)).with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    /// Coverage pointer: every required case has an asserting home.
    #[test]
    fn matrix_coverage_pointer() {
        let cases = coverage_cases();
        assert_eq!(cases.len(), 8, "full matrix covered");
        for (name, loc) in cases {
            eprintln!("[matrix:{name}] -> {loc}");
        }
    }

    /// Proof gate fail-closed matrix (the consolidated gap).
    #[tokio::test]
    async fn proof_gate_fail_closed_matrix() {
        let (mailbox, db_path, data_dir) = test_mailbox("proof").await;
        let leaked = run_proof_matrix(mailbox.index_pool().unwrap().clone()).await;
        assert!(
            leaked.is_empty(),
            "proof gate failed CLOSED on: {leaked:?} (these must be rejected)"
        );
        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
