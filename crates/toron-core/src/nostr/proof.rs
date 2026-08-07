// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Proof gate — Schnorr-native kind-30012 attestation + Ed25519 escape hatch
//! (ADR-0003 rev3 Decision 3, SC-8).
//!
//! The trust anchor is a Nostr keypair; the attestation is a signed kind-30012
//! binding the agent npub + project + scope + nonce. Single crypto, single
//! identity: the agent's npub *is* the attested subject. Ed25519 is a legacy
//! escape hatch (`TORON_PROOF_SCHEME=ed25519`) for orgs with existing CAs only.
//!
//! **Fail-closed**: missing / malformed / expired / replayed / mismatched
//! proofs are all rejected. Nonces persist across restarts (SQLite
//! `proof_nonces`), so a replayed nonce is rejected after a process restart.

use nostr_sdk::nostr::event::builder::Error as BuilderError;
use nostr_sdk::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use crate::bus::scoping::with_project_scoping;
use crate::nostr::event::build_event;
use crate::nostr::kinds::{REGISTRATION_ATTESTATION, toron_kind};

/// Proof scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProofScheme {
    /// Schnorr (default): kind-30012 signed by the Nostr trust anchor.
    #[default]
    Schnorr,
    /// Ed25519 escape hatch (legacy CAs only).
    Ed25519,
}

impl ProofScheme {
    /// Parse from `TORON_PROOF_SCHEME`.
    pub fn from_env_value(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "schnorr" => Some(Self::Schnorr),
            "ed25519" => Some(Self::Ed25519),
            _ => None,
        }
    }
}

impl std::fmt::Display for ProofScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Schnorr => "schnorr",
            Self::Ed25519 => "ed25519",
        })
    }
}

/// The attestation payload (kind-30012 `content`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Attestation {
    /// Scheme used to sign this attestation.
    pub scheme: String,
    /// The attested agent public key (hex).
    pub agent_npub: String,
    /// Project slug the agent is authorized for.
    pub project: String,
    /// Authorization scope (e.g. `"project:collaborator"`).
    pub scope: String,
    /// Single-use random nonce (hex).
    pub nonce: String,
    /// Expiry (unix seconds). Absent = no expiry (not recommended).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<u64>,
    /// Ed25519 signature over the canonical binding (ed25519 scheme only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
}

impl Attestation {
    /// The canonical bytes signed by Ed25519: `scheme|agent|project|scope|nonce|expiry`.
    fn canonical_bytes(&self) -> Vec<u8> {
        format!(
            "{}|{}|{}|{}|{}|{}",
            self.scheme,
            self.agent_npub,
            self.project,
            self.scope,
            self.nonce,
            self.expires_at.unwrap_or(0)
        )
        .into_bytes()
    }
}

/// Proof-gate failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProofError {
    #[error("proof missing")]
    Missing,
    #[error("proof malformed: {0}")]
    Malformed(String),
    #[error("proof expired")]
    Expired,
    #[error("proof nonce replayed")]
    Replayed,
    #[error("proof mismatch: {0}")]
    Mismatch(String),
    #[error("bad signature")]
    BadSignature,
    #[error("unsupported proof scheme: {0}")]
    UnsupportedScheme(String),
    #[error("nonce store failure: {0}")]
    Store(String),
}

impl From<anyhow::Error> for ProofError {
    fn from(e: anyhow::Error) -> Self {
        Self::Store(e.to_string())
    }
}

/// Expected proof claim (what the caller asserts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProofClaim {
    /// Expected agent public key.
    pub agent: PublicKey,
    /// Expected project slug.
    pub project: String,
    /// Expected scope.
    pub scope: String,
}

impl ProofClaim {
    /// New claim.
    pub fn new(agent: PublicKey, project: impl Into<String>, scope: impl Into<String>) -> Self {
        Self {
            agent,
            project: project.into(),
            scope: scope.into(),
        }
    }
}

/// The proof gate verifier. Pure for the signature/binding checks; takes a
/// [`NonceStore`] for replay protection (may be `None` when nonces are not
/// enforced).
#[derive(Debug, Clone)]
pub struct ProofGate {
    /// Trust anchor public keys (hex) whose attestations are accepted.
    pub trusted_anchors: Vec<String>,
    /// Scheme expected per config.
    pub scheme: ProofScheme,
}

impl ProofGate {
    /// Verify a Schnorr attestation (kind-30012 event) against a claim.
    ///
    /// Fail-closed checks in order: kind, event signature+id, anchor
    /// trust, content parse, binding match (agent/project/scope), expiry,
    /// nonce replay.
    pub async fn verify_schnorr(
        &self,
        proof: &Event,
        claim: &ProofClaim,
        nonce_store: &mut NonceStore,
        now: Timestamp,
    ) -> Result<(), ProofError> {
        if proof.kind.as_u16() != REGISTRATION_ATTESTATION {
            return Err(ProofError::Mismatch("wrong kind".into()));
        }
        // Verify event signature + canonical id (forged/mutated rejected).
        crate::nostr::event::verify_event(proof).map_err(|_| ProofError::BadSignature)?;
        // Anchor trust.
        let anchor_hex = proof.pubkey.to_hex();
        if !self.trusted_anchors.iter().any(|a| a == &anchor_hex) {
            return Err(ProofError::Mismatch(format!(
                "untrusted anchor {anchor_hex}"
            )));
        }
        // Content parse.
        let att: Attestation = serde_json::from_str(&proof.content)
            .map_err(|e| ProofError::Malformed(format!("attestation json: {e}")))?;
        if att.scheme != "schnorr" {
            return Err(ProofError::Mismatch("scheme is not schnorr".into()));
        }
        self.check_binding_and_expiry(&att, claim, now)?;
        self.consume_nonce(&att, nonce_store).await?;
        Ok(())
    }

    /// Verify an Ed25519 attestation against a claim.
    pub async fn verify_ed25519(
        &self,
        proof: &str,
        claim: &ProofClaim,
        nonce_store: &mut NonceStore,
        now: Timestamp,
        trusted_ed25519: &[ed25519_dalek::VerifyingKey],
    ) -> Result<(), ProofError> {
        let att: Attestation =
            serde_json::from_str(proof).map_err(|e| ProofError::Malformed(format!("{e}")))?;
        if att.scheme != "ed25519" {
            return Err(ProofError::Mismatch("scheme is not ed25519".into()));
        }
        let sig_hex = att
            .signature
            .as_ref()
            .ok_or(ProofError::Malformed("missing ed25519 signature".into()))?;
        let sig_bytes =
            hex::decode(sig_hex).map_err(|_| ProofError::Malformed("bad signature hex".into()))?;
        let sig = ed25519_dalek::Signature::from_slice(&sig_bytes)
            .map_err(|_| ProofError::BadSignature)?;
        // Trusted verifier: try each key.
        let mut ok = false;
        for vk in trusted_ed25519 {
            if vk.verify_strict(&att.canonical_bytes(), &sig).is_ok() {
                ok = true;
                break;
            }
        }
        if !ok {
            return Err(ProofError::BadSignature);
        }
        self.check_binding_and_expiry(&att, claim, now)?;
        self.consume_nonce(&att, nonce_store).await?;
        Ok(())
    }

    /// Shared binding + expiry checks.
    fn check_binding_and_expiry(
        &self,
        att: &Attestation,
        claim: &ProofClaim,
        now: Timestamp,
    ) -> Result<(), ProofError> {
        if att.agent_npub != claim.agent.to_hex() {
            return Err(ProofError::Mismatch(
                "agent npub does not match claim".into(),
            ));
        }
        if att.project != claim.project {
            return Err(ProofError::Mismatch("project does not match claim".into()));
        }
        if att.scope != claim.scope {
            return Err(ProofError::Mismatch("scope does not match claim".into()));
        }
        if let Some(exp) = att.expires_at
            && exp < now.as_secs()
        {
            return Err(ProofError::Expired);
        }
        Ok(())
    }

    async fn consume_nonce(
        &self,
        att: &Attestation,
        store: &mut NonceStore,
    ) -> Result<(), ProofError> {
        if store.enabled
            && !store
                .consume(&att.nonce, &att.agent_npub, &att.scope)
                .await?
        {
            return Err(ProofError::Replayed);
        }
        Ok(())
    }
}

/// Nonce store backed by the `proof_nonces` table (persists across restarts).
#[derive(Debug, Clone)]
pub struct NonceStore {
    pool: sqlx::SqlitePool,
    /// When false, replay protection is disabled (gate tests / nonce-less).
    pub enabled: bool,
}

impl NonceStore {
    /// Open the store on a pool.
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool,
            enabled: true,
        }
    }

    /// Disable replay protection (used by pure/gate-off tests).
    pub fn disabled(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool,
            enabled: false,
        }
    }

    /// Attempt to consume a nonce. Returns `Ok(true)` the first time, and
    /// `Ok(false)` (replayed) on any repeat — even across restarts, because
    /// the row persists.
    pub async fn consume(&self, nonce: &str, pubkey: &str, scope: &str) -> anyhow::Result<bool> {
        let now_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);
        let result = sqlx::query(
            "INSERT INTO proof_nonces (nonce, pubkey, scope, used, created_ts_us, used_ts_us)
             VALUES (?1, ?2, ?3, 1, ?4, ?5)
             ON CONFLICT (nonce) DO NOTHING",
        )
        .bind(nonce)
        .bind(pubkey)
        .bind(scope)
        .bind(now_us)
        .bind(now_us)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() == 1)
    }

    /// Has a nonce been consumed already?
    pub async fn is_consumed(&self, nonce: &str) -> anyhow::Result<bool> {
        let row = sqlx::query("SELECT 1 FROM proof_nonces WHERE nonce = ?1")
            .bind(nonce)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.is_some())
    }
}

/// Build a kind-30012 Schnorr attestation event (trust-anchor signed).
pub fn build_attestation_event(
    anchor: &Keys,
    att: &Attestation,
    project_slug: &str,
    name: &str,
    created_at: Timestamp,
) -> Result<Event, BuilderError> {
    let content = serde_json::to_string(att).expect("attestation serializes");
    let base_tags = vec![Tag::parse(["d", &format!("{project_slug}:{name}")]).expect("d")];
    let tags = with_project_scoping(
        toron_kind(REGISTRATION_ATTESTATION),
        base_tags,
        Some(project_slug),
    );
    build_event(
        anchor,
        toron_kind(REGISTRATION_ATTESTATION),
        content,
        tags,
        created_at,
    )
}

/// Convenience for the doc example / E2E: an attestation's event id as hex.
pub fn attestation_event_id(event: &Event) -> EventId {
    event.id
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn temp_pool() -> (sqlx::SqlitePool, std::path::PathBuf) {
        let path =
            std::env::temp_dir().join(format!("toron-proof-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = crate::index::schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    async fn make_store() -> (NonceStore, std::path::PathBuf) {
        let (pool, path) = temp_pool().await;
        (NonceStore::new(pool), path)
    }

    fn now() -> Timestamp {
        Timestamp::from(1_800_000_000_u64)
    }

    fn attest(agent: &PublicKey, nonce: &str) -> Attestation {
        Attestation {
            scheme: "schnorr".into(),
            agent_npub: agent.to_hex(),
            project: "demo".into(),
            scope: "project:collaborator".into(),
            nonce: nonce.into(),
            expires_at: Some(1_800_000_100_u64),
            signature: None,
        }
    }

    #[test]
    fn test_scheme_env_parse() {
        assert_eq!(
            ProofScheme::from_env_value("schnorr"),
            Some(ProofScheme::Schnorr)
        );
        assert_eq!(
            ProofScheme::from_env_value("ed25519"),
            Some(ProofScheme::Ed25519)
        );
        assert_eq!(
            ProofScheme::from_env_value("ED25519"),
            Some(ProofScheme::Ed25519)
        );
        assert_eq!(ProofScheme::from_env_value("banana"), None);
        assert_eq!(ProofScheme::default(), ProofScheme::Schnorr);
    }

    #[tokio::test]
    async fn test_valid_schnorr_attestation_accepted() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let att = attest(&agent.public_key(), "nonce-1");
        let ev = build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        gate.verify_schnorr(&ev, &claim, &mut store, now())
            .await
            .expect("valid attestation accepted");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_replay_nonce_rejected() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let att = attest(&agent.public_key(), "nonce-replay");
        let ev = build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        gate.verify_schnorr(&ev, &claim, &mut store, now())
            .await
            .expect("first use ok");
        match gate.verify_schnorr(&ev, &claim, &mut store, now()).await {
            Err(ProofError::Replayed) => {}
            other => panic!("expected Replayed, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_nonce_persists_across_store_reopen() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        // Reopen = new NonceStore on the SAME sqlite file (restart sim).
        let (pool, path) = temp_pool().await;
        {
            let mut store = NonceStore::new(pool.clone());
            let att = attest(&agent.public_key(), "nonce-persist");
            let ev =
                build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
            let gate = ProofGate {
                trusted_anchors: vec![anchor.public_key().to_hex()],
                scheme: ProofScheme::Schnorr,
            };
            let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
            gate.verify_schnorr(&ev, &claim, &mut store, now())
                .await
                .expect("first ok");
        }
        // New store instance on same DB — nonce already consumed.
        let store2 = NonceStore::new(pool);
        assert!(store2.is_consumed("nonce-persist").await.expect("query"));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_expired_attestation_rejected() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let mut att = attest(&agent.public_key(), "nonce-expired");
        att.expires_at = Some(1_700_000_000_u64); // past now()=1_800_000_000
        let ev = build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        match gate.verify_schnorr(&ev, &claim, &mut store, now()).await {
            Err(ProofError::Expired) => {}
            other => panic!("expected Expired, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_mismatched_agent_rejected() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let other = Keys::generate();
        let (mut store, path) = make_store().await;

        let att = attest(&agent.public_key(), "nonce-mismatch");
        let ev = build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        // Claim wrong agent.
        let claim = ProofClaim::new(other.public_key(), "demo", "project:collaborator");
        match gate.verify_schnorr(&ev, &claim, &mut store, now()).await {
            Err(ProofError::Mismatch(_)) => {}
            other => panic!("expected Mismatch, got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_mismatched_project_or_scope_rejected() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let att = attest(&agent.public_key(), "nonce-mismatch2");
        let ev = build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "other-project", "project:collaborator");
        assert!(matches!(
            gate.verify_schnorr(&ev, &claim, &mut store, now()).await,
            Err(ProofError::Mismatch(_))
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_untrusted_anchor_rejected() {
        let anchor = Keys::generate();
        let rogue = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let att = attest(&agent.public_key(), "nonce-rogue");
        let ev = build_attestation_event(&rogue, &att, "demo", "BlueLake", now()).expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        match gate.verify_schnorr(&ev, &claim, &mut store, now()).await {
            Err(ProofError::Mismatch(_)) => {}
            other => panic!("expected Mismatch (untrusted anchor), got {other:?}"),
        }
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_forged_attestation_rejected() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let att = attest(&agent.public_key(), "nonce-forged");
        let ev = build_attestation_event(&anchor, &att, "demo", "BlueLake", now()).expect("build");
        // Forge: mutate content but keep id+sig.
        let forged = Event::new(
            ev.id,
            ev.pubkey,
            ev.created_at,
            ev.kind,
            ev.tags.clone(),
            "{\"scheme\":\"schnorr\",\"agent_npub\":\"fake\"}",
            ev.sig,
        );
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        assert!(
            gate.verify_schnorr(&forged, &claim, &mut store, now())
                .await
                .is_err()
        );
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_malformed_content_rejected() {
        let anchor = Keys::generate();
        let agent = Keys::generate();
        let (mut store, path) = make_store().await;

        let malformed = build_event(
            &anchor,
            toron_kind(REGISTRATION_ATTESTATION),
            "not-json",
            vec![Tag::parse(["d", "demo:BlueLake"]).expect("d")],
            now(),
        )
        .expect("build");
        let gate = ProofGate {
            trusted_anchors: vec![anchor.public_key().to_hex()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        assert!(matches!(
            gate.verify_schnorr(&malformed, &claim, &mut store, now())
                .await,
            Err(ProofError::Malformed(_))
        ));
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn test_ed25519_roundtrip_verify() {
        use ed25519_dalek::{Signer, SigningKey};
        // Deterministic key (no rand dep needed).
        let signer = SigningKey::from_bytes(&[7u8; 32]);
        let vk = signer.verifying_key();

        let agent = Keys::generate();
        let mut att = Attestation {
            scheme: "ed25519".into(),
            agent_npub: agent.public_key().to_hex(),
            project: "demo".into(),
            scope: "project:collaborator".into(),
            nonce: "ed-nonce-1".into(),
            expires_at: Some(1_800_000_100_u64),
            signature: None,
        };
        let sig = signer.sign(&att.canonical_bytes());
        att.signature = Some(hex::encode(sig.to_bytes()));

        let gate = ProofGate {
            trusted_anchors: vec![],
            scheme: ProofScheme::Ed25519,
        };
        // Use a disabled store (no sqlite in this sync test).
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("mem pool");
        let mut store = NonceStore::disabled(pool);
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        let json = serde_json::to_string(&att).expect("json");
        gate.verify_ed25519(&json, &claim, &mut store, now(), &[vk])
            .await
            .expect("ed25519 attestation accepted");
    }

    #[tokio::test]
    async fn test_ed25519_wrong_key_rejected() {
        use ed25519_dalek::{Signer, SigningKey};
        let signer = SigningKey::from_bytes(&[7u8; 32]);
        let other = SigningKey::from_bytes(&[9u8; 32]);

        let agent = Keys::generate();
        let mut att = Attestation {
            scheme: "ed25519".into(),
            agent_npub: agent.public_key().to_hex(),
            project: "demo".into(),
            scope: "project:collaborator".into(),
            nonce: "ed-nonce-2".into(),
            expires_at: Some(1_800_000_100_u64),
            signature: None,
        };
        let sig = signer.sign(&att.canonical_bytes());
        att.signature = Some(hex::encode(sig.to_bytes()));

        let gate = ProofGate {
            trusted_anchors: vec![],
            scheme: ProofScheme::Ed25519,
        };
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("mem pool");
        let mut store = NonceStore::disabled(pool);
        let claim = ProofClaim::new(agent.public_key(), "demo", "project:collaborator");
        let json = serde_json::to_string(&att).expect("json");
        // Only `other` is trusted → sig by `signer` rejected.
        assert!(matches!(
            gate.verify_ed25519(&json, &claim, &mut store, now(), &[other.verifying_key()])
                .await,
            Err(ProofError::BadSignature)
        ));
    }
}
