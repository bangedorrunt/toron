// governed-by: ADR-0037
//! ADR-0037 W2: one pin, one key — reuse at registration, behind proof of
//! possession.
//!
//! A pin already registered in another project never gets a second key.
//! Registering it in a new project REUSES its key, and only after the
//! caller proves it holds that key: a kind-30012 attestation (the existing
//! registration-attestation kind, ADR-0003 rev3 D3; no new kind) signed BY
//! the pin's key, binding the pin's npub, the NEW project and the scope
//! `adr0037:pin-reuse:<pin>`, with a single-use nonce and a 10-minute
//! expiry. The anchor is the key the `agents` index binds to the pin in the
//! source project — never the key the caller hands over.
//!
//! The MCP surface is frozen (ADR-0033), so `register_agent` over MCP cannot
//! carry a proof: it refuses and names the pin. The host-local CLI
//! (`toron agent bootstrap`) proves possession by unlocking the source
//! project's key file, then registers through [`Mailbox::register_agent_reusing`].

use crate::nostr::identity::{self as ident, AgentProfile};
use crate::nostr::proof::{
    Attestation, NonceStore, ProofClaim, ProofGate, ProofScheme, build_attestation_event,
};
use crate::nostr::{Event, PublicKey, Timestamp};

use super::Mailbox;
use super::identity::RegisteredAgent;

/// Scope prefix of a pin-reuse attestation (`<prefix>:<pin>`).
pub const PIN_REUSE_SCOPE: &str = "adr0037:pin-reuse";
/// How long a reuse proof stays valid.
pub const PIN_REUSE_TTL_SECS: u64 = 600;

fn scope_for(pin: &str) -> String {
    format!("{PIN_REUSE_SCOPE}:{pin}")
}

impl Mailbox {
    /// The pin's bindings in projects OTHER than `project_slug` (canonical).
    pub async fn pin_bindings_elsewhere(
        &self,
        project_slug: &str,
        pin: &str,
    ) -> anyhow::Result<Vec<(String, String)>> {
        let canonical = self.canonical_project(project_slug).await;
        let mut out: Vec<(String, String)> = self
            .pin_bindings(pin)
            .await?
            .into_iter()
            .filter(|(p, _)| *p != canonical)
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// The refusal a key-minting registration gets when the pin is already
    /// bound elsewhere (W2). Names the pin, where it lives, and the way in.
    pub(crate) async fn refuse_second_key(
        &self,
        project_slug: &str,
        pin: &str,
    ) -> anyhow::Result<()> {
        let elsewhere = self.pin_bindings_elsewhere(project_slug, pin).await?;
        if elsewhere.is_empty() {
            return Ok(());
        }
        let at: Vec<String> = elsewhere
            .iter()
            .map(|(p, k)| format!("{p} ({}…)", &k[..12.min(k.len())]))
            .collect();
        anyhow::bail!(
            "pin {pin} is already registered in {} — one pin, one key (ADR-0037): it cannot \
             get a second key in project {project_slug}. Reuse its key with proof of \
             possession: run `toron agent bootstrap --project {project_slug} --as {pin}` on \
             the host that holds the key{}.",
            at.join(", "),
            if elsewhere.len() > 1 {
                " and pick the key with --key-from <project> (the pin is split, ADR-0037 §5)"
            } else {
                ""
            }
        )
    }

    /// The project to reuse `pin`'s key from when registering it in
    /// `project_slug`: `None` when the pin is unregistered elsewhere (mint)
    /// or already has a key here (upsert). A split pin needs `key_from`.
    pub async fn pin_reuse_source(
        &self,
        project_slug: &str,
        pin: &str,
        key_from: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let canonical = self.canonical_project(project_slug).await;
        if self
            .load_agent_key(&self.agent_key_path(&canonical, pin))
            .is_some()
        {
            return Ok(None);
        }
        let elsewhere = self.pin_bindings_elsewhere(&canonical, pin).await?;
        if let Some(from) = key_from {
            let from = self.canonical_project(from).await;
            if !elsewhere.iter().any(|(p, _)| *p == from) {
                anyhow::bail!("--key-from {from}: pin {pin} is not registered in project {from}");
            }
            return Ok(Some(from));
        }
        let mut keys: Vec<&String> = elsewhere.iter().map(|(_, k)| k).collect();
        keys.sort();
        keys.dedup();
        match keys.len() {
            0 => Ok(None),
            1 => Ok(elsewhere.first().map(|(p, _)| p.clone())),
            _ => {
                let projects: Vec<&str> = elsewhere.iter().map(|(p, _)| p.as_str()).collect();
                anyhow::bail!(
                    "pin {pin} holds {} different keys (in {}); pick the one to keep with \
                     --key-from <project> (ADR-0037 §5)",
                    keys.len(),
                    projects.join(", ")
                )
            }
        }
    }

    /// Sign a pin-reuse proof with the pin's key from `from_project`. The
    /// host-local half of W2: only a caller that can unlock that key file
    /// can produce it.
    pub async fn prove_pin_possession(
        &self,
        from_project: &str,
        pin: &str,
        for_project: &str,
    ) -> anyhow::Result<Event> {
        let from = self.canonical_project(from_project).await;
        let to = self.canonical_project(for_project).await;
        let keys = self.agent_keys_for(&from, pin)?;
        let now = Timestamp::now();
        let att = Attestation {
            scheme: "schnorr".into(),
            agent_npub: keys.public_key().to_hex(),
            project: to.clone(),
            scope: scope_for(pin),
            nonce: uuid::Uuid::new_v4().simple().to_string(),
            expires_at: Some(now.as_secs() + PIN_REUSE_TTL_SECS),
            signature: None,
        };
        Ok(build_attestation_event(&keys, &att, &to, pin, now)?)
    }

    /// Register `profile` in `project_slug` REUSING the key the pin holds in
    /// `from_project`, after verifying `proof` (W2). Fail-closed: a missing
    /// source binding, a proof signed by any other key, a proof for another
    /// project or pin, an expired or replayed proof are all refused, and the
    /// refusal names the pin.
    pub async fn register_agent_reusing(
        &self,
        project_slug: &str,
        profile: &AgentProfile,
        from_project: &str,
        proof: &Event,
    ) -> anyhow::Result<RegisteredAgent> {
        let canonical = self.canonical_project(project_slug).await;
        let from = self.canonical_project(from_project).await;
        let pin = &profile.name;
        let anchor = self
            .pin_binding_in(&from, pin)
            .await?
            .ok_or_else(|| anyhow::anyhow!("pin {pin} is not registered in project {from}"))?;
        let pool = self
            .index_pool()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("pin reuse needs the index pool (nonce store)"))?;
        let gate = ProofGate {
            trusted_anchors: vec![anchor.clone()],
            scheme: ProofScheme::Schnorr,
        };
        let claim = ProofClaim::new(
            PublicKey::from_hex(&anchor)?,
            canonical.clone(),
            scope_for(pin),
        );
        gate.verify_schnorr(proof, &claim, &mut NonceStore::new(pool), Timestamp::now())
            .await
            .map_err(|e| anyhow::anyhow!("proof of possession for pin {pin} refused: {e}"))?;
        // Proven: the custodian copy of that key becomes this project's key.
        let keys = self.agent_keys_for(&from, pin)?;
        if keys.public_key().to_hex() != anchor {
            anyhow::bail!("pin {pin}: the key file in {from} does not match its registered key");
        }
        if let Some(here) = self.load_agent_key(&self.agent_key_path(&canonical, pin))
            && here.public_key() != keys.public_key()
        {
            anyhow::bail!(
                "pin {pin} already holds a DIFFERENT key in project {canonical} (a legacy split, \
                 ADR-0037 §5): collapsing it is an operator decision, not a registration"
            );
        }
        let result = ident::register_agent(&canonical, profile, Some(&keys), Timestamp::now())?;
        let mut agent = self
            .persist_and_publish(&canonical, profile, result)
            .await?;
        agent.created = false;
        Ok(agent)
    }
}

#[cfg(test)]
mod tests {
    //! ADR-0037 §7.3 / §8 named tests for W2–W4.
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::messaging::{FetchInbox, SendMessage};
    use std::sync::Arc;

    const PASS: &str = "pin-reuse-test-pass";

    async fn mailbox(tag: &str) -> Mailbox {
        let db = std::env::temp_dir().join(format!(
            "toron-pinreuse-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data = std::env::temp_dir().join(format!(
            "toron-pinreuse-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        Mailbox::new(Arc::new(bus), data, Some(crate::keys::generate()))
            .with_agent_passphrase(PASS)
            .with_index_pool(pool)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("test".into()),
            model: None,
            persona: None,
            task_description: None,
        }
    }

    /// W2: registering a known pin in a new project returns the SAME npub.
    #[tokio::test]
    async fn register_reuses_a_known_pins_key() {
        let mb = mailbox("reuse").await;
        let a = mb
            .register_agent("alpha", &profile("AmberFox"))
            .await
            .expect("alpha");
        assert_eq!(
            mb.pin_reuse_source("beta", "AmberFox", None)
                .await
                .unwrap()
                .as_deref(),
            Some("alpha")
        );
        let proof = mb
            .prove_pin_possession("alpha", "AmberFox", "beta")
            .await
            .expect("prove");
        let b = mb
            .register_agent_reusing("beta", &profile("AmberFox"), "alpha", &proof)
            .await
            .expect("reuse");
        assert_eq!(b.npub, a.npub, "the SAME npub, not a second key");
        assert!(!b.created);
        // Idempotent afterwards: a plain upsert in beta keeps the key.
        let again = mb
            .register_agent("beta", &profile("AmberFox"))
            .await
            .expect("upsert");
        assert_eq!(again.npub, a.npub);
        assert_eq!(
            mb.pin_reuse_source("beta", "AmberFox", None).await.unwrap(),
            None
        );
    }

    /// W2: no proof → refused, and the refusal names the pin. Both the
    /// MCP-reachable registrations refuse to mint a second key.
    #[tokio::test]
    async fn register_without_possession_is_refused() {
        let mb = mailbox("noproof").await;
        mb.register_agent("alpha", &profile("AmberFox"))
            .await
            .expect("alpha");
        for err in [
            mb.register_agent("beta", &profile("AmberFox"))
                .await
                .expect_err("register_agent"),
            mb.create_agent_identity("beta", &profile("AmberFox"))
                .await
                .expect_err("create_agent_identity"),
        ] {
            let e = err.to_string();
            assert!(
                e.contains("pin AmberFox") && e.contains("proof of possession"),
                "{e}"
            );
        }
        // A proof for ANOTHER project does not open this one.
        let for_gamma = mb
            .prove_pin_possession("alpha", "AmberFox", "gamma")
            .await
            .unwrap();
        let e = mb
            .register_agent_reusing("beta", &profile("AmberFox"), "alpha", &for_gamma)
            .await
            .expect_err("wrong project")
            .to_string();
        assert!(e.contains("pin AmberFox") && e.contains("refused"), "{e}");
    }

    /// W2: a proof signed by any key but the pin's is refused, and a valid
    /// proof cannot be replayed.
    #[tokio::test]
    async fn register_with_wrong_signature_is_refused() {
        let mb = mailbox("wrongsig").await;
        mb.register_agent("alpha", &profile("AmberFox"))
            .await
            .expect("alpha");
        mb.register_agent("alpha", &profile("BirchOwl"))
            .await
            .expect("other pin");
        // BirchOwl's key signs a claim for AmberFox's npub: wrong signer.
        let birch = mb.agent_keys_for("alpha", "BirchOwl").unwrap();
        let amber_hex = mb
            .agent_keys_for("alpha", "AmberFox")
            .unwrap()
            .public_key()
            .to_hex();
        let forged = build_attestation_event(
            &birch,
            &Attestation {
                scheme: "schnorr".into(),
                agent_npub: amber_hex,
                project: "beta".into(),
                scope: scope_for("AmberFox"),
                nonce: "forged-1".into(),
                expires_at: Some(Timestamp::now().as_secs() + 60),
                signature: None,
            },
            "beta",
            "AmberFox",
            Timestamp::now(),
        )
        .unwrap();
        let e = mb
            .register_agent_reusing("beta", &profile("AmberFox"), "alpha", &forged)
            .await
            .expect_err("forged")
            .to_string();
        assert!(e.contains("pin AmberFox") && e.contains("refused"), "{e}");
        // Tampered content (signature no longer matches) is refused too.
        let mut tampered = mb
            .prove_pin_possession("alpha", "AmberFox", "beta")
            .await
            .unwrap();
        tampered.content = tampered.content.replace("beta", "beta2");
        assert!(
            mb.register_agent_reusing("beta", &profile("AmberFox"), "alpha", &tampered)
                .await
                .is_err()
        );
        // Replay: the first use passes, the second is refused.
        let good = mb
            .prove_pin_possession("alpha", "AmberFox", "beta")
            .await
            .unwrap();
        mb.register_agent_reusing("beta", &profile("AmberFox"), "alpha", &good)
            .await
            .expect("first use");
        let e = mb
            .register_agent_reusing("beta", &profile("AmberFox"), "alpha", &good)
            .await
            .expect_err("replay")
            .to_string();
        assert!(e.contains("replayed"), "{e}");
    }

    async fn send(mb: &Mailbox, project: &str, from: &str, to: &str) -> anyhow::Result<()> {
        mb.send_message(&SendMessage {
            project_slug: project.into(),
            sender_name: from.into(),
            to: vec![to.into()],
            subject: format!("to {to}"),
            body_md: "hello".into(),
            ..Default::default()
        })
        .await
        .map(|_| ())
    }

    async fn inbox_subjects(mb: &Mailbox, project: &str, who: &str) -> Vec<String> {
        for _ in 0..50 {
            let rows = mb
                .fetch_inbox(&FetchInbox {
                    project_slug: project.into(),
                    agent_name: who.into(),
                    include_bodies: true,
                    ..Default::default()
                })
                .await
                .expect("inbox");
            if !rows.is_empty() {
                return rows.into_iter().map(|m| m.subject).collect();
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Vec::new()
    }

    /// W3: a send from project A to a pin registered only in B is readable
    /// in B's inbox (bd-j5sk's end-to-end proof), and a different pin's
    /// inbox never sees it.
    #[tokio::test]
    async fn cross_project_send_is_readable_by_the_recipient() {
        let mb = mailbox("xproj").await;
        mb.register_agent("alpha", &profile("SenderJay"))
            .await
            .unwrap();
        mb.register_agent("beta", &profile("ReaderOwl"))
            .await
            .unwrap();
        mb.register_agent("beta", &profile("OtherElk"))
            .await
            .unwrap();
        send(&mb, "alpha", "SenderJay", "ReaderOwl")
            .await
            .expect("cross-project send delivers");
        assert_eq!(
            inbox_subjects(&mb, "beta", "ReaderOwl").await,
            vec!["to ReaderOwl".to_string()]
        );
        assert!(
            inbox_subjects(&mb, "beta", "OtherElk").await.is_empty(),
            "never delivered to another pin"
        );
    }

    /// W3/W4: an unregistered name still fails loud, bare or qualified.
    #[tokio::test]
    async fn unknown_recipient_still_fails_loud() {
        let mb = mailbox("unknown").await;
        mb.register_agent("alpha", &profile("SenderJay"))
            .await
            .unwrap();
        for to in ["NobodyHere", "NobodyHere@alpha", "NobodyHere@nosuchproject"] {
            let e = send(&mb, "alpha", "SenderJay", to)
                .await
                .expect_err(to)
                .to_string();
            assert!(e.contains("unknown recipient"), "{to}: {e}");
        }
    }

    /// W4: `pin@project` resolves to that project's key; an unknown project
    /// suffix falls through to the literal-name path and fails; the bare
    /// form of a split pin refuses and names the qualified forms.
    #[tokio::test]
    async fn qualified_address_resolves_and_unknown_project_fails() {
        let mb = mailbox("qualified").await;
        mb.register_agent("alpha", &profile("SenderJay"))
            .await
            .unwrap();
        mb.register_agent("beta", &profile("SplitCat"))
            .await
            .unwrap();
        // A legacy split, written as it existed before W2 (triggers off for
        // the fixture only, then back on).
        let pool = mb.index_pool().unwrap().clone();
        sqlx::query("DROP TRIGGER agents_one_key_per_pin_insert")
            .execute(&pool)
            .await
            .unwrap();
        let legacy = crate::keys::generate();
        crate::keys::write_encrypted_key(
            &mb.agent_key_path("gamma", "SplitCat"),
            &legacy,
            PASS,
            crate::keys::DEFAULT_LOG_N,
        )
        .unwrap();
        sqlx::query("INSERT INTO agents (project_slug, name, pubkey, created_ts_us) VALUES ('gamma', 'SplitCat', ?1, 1)")
            .bind(legacy.public_key().to_hex())
            .execute(&pool)
            .await
            .unwrap();
        schema::migrate(&pool).await.ok();
        sqlx::query(include_str!(
            "../../migrations/0020_agents_one_key_per_pin.sql"
        ))
        .execute(&pool)
        .await
        .ok();

        let e = send(&mb, "alpha", "SenderJay", "SplitCat")
            .await
            .expect_err("bare split")
            .to_string();
        assert!(
            e.contains("SplitCat@beta") && e.contains("SplitCat@gamma") && e.contains("Refusing"),
            "{e}"
        );
        send(&mb, "alpha", "SenderJay", "SplitCat@beta")
            .await
            .expect("qualified send");
        assert_eq!(
            inbox_subjects(&mb, "beta", "SplitCat").await,
            vec!["to SplitCat@beta".to_string()]
        );
        assert!(
            inbox_subjects(&mb, "gamma", "SplitCat").await.is_empty(),
            "the other key never sees it"
        );
        let e = send(&mb, "alpha", "SenderJay", "SplitCat@nosuchproject")
            .await
            .expect_err("unknown suffix")
            .to_string();
        assert!(
            e.contains("unknown recipient: SplitCat@nosuchproject"),
            "{e}"
        );
    }
}
