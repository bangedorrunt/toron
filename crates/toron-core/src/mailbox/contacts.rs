// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Contacts cluster (bd-led.5.4): request_contact, respond_contact,
//! set_contact_policy, list_contacts — G14.
//!
//! ADR-0003 G14: the **bus is dumb transport**; contact policies are enforced
//! at the projection (bd-led.4.4 unwrap pipeline). These tools SET the state:
//! - `request_contact` / `respond_contact` → NIP-17 control messages + an
//!   AgentLink row in the index (project_slug, from/to name+npub, status).
//! - `set_contact_policy` → addressable **kind-30014** (`d`=project-slug:name)
//!   whose content is `open|auto|contacts_only|block_all`.
//! - `list_contacts` → AgentLink rows for an agent.
//!
//! Cross-project (G2): an AgentLink carries the peer npub; resolution hops
//! through it when the peer is in another project.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use nostr_sdk::nostr::{Keys, PublicKey, Tag, Timestamp};
use sqlx::Row;

use crate::bus::filter::kind_project_filter;
use crate::bus::scoping::with_project_scoping;
use crate::mailbox::unwrap::ContactPolicy;
use crate::nostr::kinds::{AGENT_PROFILE, CONTACT_POLICY, toron_kind};

use super::Mailbox;
use super::collect_replay;
/// Status of an AgentLink — the SOCIAL GRAPH, not a delivery gate
/// (ADR-0003 G14; S7 bd-contact-semantics-hs08).
///
/// - `pending`: request sent, not yet answered. Mail to a `pending` target
///   STILL DELIVERS: the sender-side gate is the recipient being registered;
///   the recipient-side gate is their contact policy (kind-30014, default
///   `auto`), applied on INBOUND projection. `pending` never blocks.
/// - `approved`: the peer accepted the request.
/// - `denied`: the peer rejected the request (social statement; delivery
///   rules unchanged — policy still governs).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LinkStatus {
    Pending,
    Approved,
    Denied,
}

impl LinkStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }

    fn from_str(s: &str) -> Self {
        match s {
            "approved" => Self::Approved,
            "denied" => Self::Denied,
            _ => Self::Pending,
        }
    }
}

/// A contact link (AgentLink) between two agents. Link rows describe the
/// social graph; they do NOT gate mail. Inbound delivery is gated by the
/// recipient's contact policy (kind-30014, [`ContactPolicy`]) — see
/// [`LinkStatus`] for the pending-does-not-block contract.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ContactLink {
    pub project_slug: String,
    pub from_name: String,
    pub from_pubkey: String,
    pub to_name: String,
    pub to_pubkey: String,
    pub to_project: Option<String>,
    /// `pending` | `approved` | `denied` — social graph state, not a
    /// delivery gate (pending links still receive mail).
    pub status: LinkStatus,
    pub created_ts_us: i64,
}

/// `request_contact` args.
#[derive(Debug, Clone)]
pub struct RequestContact {
    pub project_slug: String,
    pub from_agent: String,
    pub to_agent: String,
    pub to_project: Option<String>,
    pub reason: Option<String>,
}

/// `respond_contact` args.
#[derive(Debug, Clone)]
pub struct RespondContact {
    pub project_slug: String,
    pub to_agent: String,
    pub from_agent: String,
    pub accept: bool,
    pub from_project: Option<String>,
}

impl Mailbox {
    /// `request_contact`: send a NIP-17 control message to the peer + record a
    /// pending AgentLink in the index.
    pub async fn request_contact(&self, args: &RequestContact) -> anyhow::Result<ContactLink> {
        let from_keys = self.agent_keys_for(&args.project_slug, &args.from_agent)?;
        let to_project = args.to_project.as_deref().unwrap_or(&args.project_slug);
        let to_npub = self.resolve_agent_npub(to_project, &args.to_agent).await?;

        let link = ContactLink {
            project_slug: args.project_slug.clone(),
            from_name: args.from_agent.clone(),
            from_pubkey: from_keys.public_key().to_hex(),
            to_name: args.to_agent.clone(),
            to_pubkey: to_npub.to_hex(),
            to_project: args.to_project.clone(),
            status: LinkStatus::Pending,
            created_ts_us: now_us(),
        };
        self.upsert_link(&link).await?;

        // NIP-17 control message: toron-kind=contact-request + p to peer.
        let mut tags = vec![Tag::parse(["toron-kind", "contact-request"]).expect("kind")];
        tags.push(Tag::parse(["p", &to_npub.to_hex()]).expect("p"));
        if let Some(reason) = &args.reason {
            tags.push(Tag::parse(["reason", reason]).expect("reason"));
        }
        self.send_control(&from_keys, &to_npub, tags).await?;
        Ok(link)
    }

    /// `respond_contact`: approve/deny + send the NIP-17 control reply.
    pub async fn respond_contact(&self, args: &RespondContact) -> anyhow::Result<ContactLink> {
        let from_project = args.from_project.as_deref().unwrap_or(&args.project_slug);
        let from_npub = self
            .resolve_agent_npub(from_project, &args.from_agent)
            .await?;
        let to_keys = self.agent_keys_for(&args.project_slug, &args.to_agent)?;

        let status = if args.accept {
            LinkStatus::Approved
        } else {
            LinkStatus::Denied
        };

        let pool = self.index_pool_required()?;
        let now = now_us();
        let result = sqlx::query(
            "UPDATE contacts SET status = ?1, created_ts_us = ?2 \
             WHERE from_pubkey = ?3 AND to_pubkey = ?4",
        )
        .bind(status.as_str())
        .bind(now)
        .bind(from_npub.to_hex())
        .bind(to_keys.public_key().to_hex())
        .execute(pool)
        .await?;
        if result.rows_affected() == 0 {
            anyhow::bail!(
                "no pending contact request from {} to {}",
                args.from_agent,
                args.to_agent
            );
        }

        // NIP-17 control reply to the requester.
        let mut tags = vec![
            Tag::parse(["toron-kind", "contact-response"]).expect("kind"),
            Tag::parse(["p", &from_npub.to_hex()]).expect("p"),
            Tag::parse(["contact-status", status.as_str()]).expect("status"),
        ];
        if let Some(proj) = &args.from_project {
            tags.push(Tag::parse(["to-project", proj]).expect("to-project"));
        }
        self.send_control(&to_keys, &from_npub, tags).await?;

        Ok(ContactLink {
            project_slug: args.project_slug.clone(),
            from_name: args.from_agent.clone(),
            from_pubkey: from_npub.to_hex(),
            to_name: args.to_agent.clone(),
            to_pubkey: to_keys.public_key().to_hex(),
            to_project: args.from_project.clone(),
            status,
            created_ts_us: now,
        })
    }

    /// `set_contact_policy`: publish the addressable kind-30014 policy event
    /// (`d`=project-slug:name, content = `open|auto|contacts_only|block_all`).
    pub async fn set_contact_policy(
        &self,
        project_slug: &str,
        agent_name: &str,
        policy: ContactPolicy,
    ) -> anyhow::Result<String> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let d = format!("{project_slug}:{agent_name}");
        let base_tags = vec![Tag::parse(["d", &d]).expect("d tag")];
        let tags = with_project_scoping(toron_kind(CONTACT_POLICY), base_tags, Some(project_slug));
        let event = crate::nostr::event::build_event(
            &agent_keys,
            toron_kind(CONTACT_POLICY),
            policy.as_str(),
            tags,
            Timestamp::now(),
        )?;
        let ok = self.bus.publish(event.clone()).await?;
        if !ok.stored {
            anyhow::bail!("contact policy event rejected by bus");
        }
        Ok(event.id.to_hex())
    }

    /// `list_contacts`: AgentLink rows involving `agent_name` in the project.
    pub async fn list_contacts(
        &self,
        project_slug: &str,
        agent_name: &str,
    ) -> anyhow::Result<Vec<ContactLink>> {
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let npub = agent_keys.public_key().to_hex();
        let pool = self.index_pool_required()?;
        let rows = sqlx::query(
            "SELECT project_slug, from_name, from_pubkey, to_name, to_pubkey, to_project, \
                    status, created_ts_us \
             FROM contacts WHERE from_pubkey = ?1 OR to_pubkey = ?1",
        )
        .bind(npub)
        .fetch_all(pool)
        .await?;
        let mut out = Vec::new();
        for row in rows {
            out.push(ContactLink {
                project_slug: row.get("project_slug"),
                from_name: row.get("from_name"),
                from_pubkey: row.get("from_pubkey"),
                to_name: row.get("to_name"),
                to_pubkey: row.get("to_pubkey"),
                to_project: row.get("to_project"),
                status: LinkStatus::from_str(row.get("status")),
                created_ts_us: row.get("created_ts_us"),
            });
        }
        Ok(out)
    }

    /// The recipient's contact policy for a pubkey (kind-30014, G14).
    ///
    /// Resolves `name` → policy by finding the kind-30014 authored by the
    /// agent (the policy's `d` is `project-slug:name`; the author IS the
    /// agent). Defaults to `Auto`.
    pub async fn contact_policy_for(
        &self,
        project_slug: &str,
        agent_npub: &PublicKey,
    ) -> ContactPolicy {
        let filter = kind_project_filter(toron_kind(CONTACT_POLICY), project_slug);
        match collect_replay(self.bus.as_ref(), filter).await {
            Ok(events) => events
                .iter()
                .filter(|e| e.pubkey == *agent_npub)
                .max_by_key(|e| e.created_at.as_secs())
                .map(|e| ContactPolicy::parse(&e.content))
                .unwrap_or(ContactPolicy::Auto),
            Err(_) => ContactPolicy::Auto,
        }
    }

    /// Is `sender` an approved contact of `recipient` (AgentLink approved)?
    pub async fn is_approved_contact(&self, sender: &PublicKey, recipient: &PublicKey) -> bool {
        let Some(pool) = self.index_pool() else {
            return false;
        };
        let row: Result<(i64,), sqlx::Error> = sqlx::query_as(
            "SELECT count(*) FROM contacts \
             WHERE ((from_pubkey = ?1 AND to_pubkey = ?2) \
                OR (from_pubkey = ?2 AND to_pubkey = ?1)) \
               AND status = 'approved'",
        )
        .bind(sender.to_hex())
        .bind(recipient.to_hex())
        .fetch_one(pool)
        .await;
        match row {
            Ok((n,)) => n > 0,
            Err(_) => false,
        }
    }

    /// Resolve a name → npub within a project (G2), or fail fast.
    ///
    /// ADR-0017: resolves across the project's full alias set (canonical +
    /// every stored alias) — a profile registered under ANY spelling of the
    /// project key is reachable from every other spelling (bd-6wfv: the raw
    /// path-form slug must not break resolution; whois does the same).
    async fn resolve_agent_npub(
        &self,
        project_slug: &str,
        name: &str,
    ) -> anyhow::Result<PublicKey> {
        let set = self.project_read_set(project_slug).await;
        for slug in &set {
            let profiles = self.project_profiles(slug).await?;
            if let Some(npub) = crate::nostr::identity::resolve_name(slug, profiles.iter(), name) {
                return Ok(npub);
            }
        }
        Err(anyhow::anyhow!(
            "unknown agent: {name} in project {project_slug}"
        ))
    }

    /// Upsert an AgentLink row (idempotent on (from_pubkey, to_pubkey)).
    async fn upsert_link(&self, link: &ContactLink) -> anyhow::Result<()> {
        let pool = self.index_pool_required()?;
        sqlx::query(
            "INSERT INTO contacts \
             (project_slug, from_name, from_pubkey, to_name, to_pubkey, to_project, \
              status, created_ts_us) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
             ON CONFLICT (from_pubkey, to_pubkey) \
             DO UPDATE SET status = excluded.status, created_ts_us = excluded.created_ts_us",
        )
        .bind(&link.project_slug)
        .bind(&link.from_name)
        .bind(&link.from_pubkey)
        .bind(&link.to_name)
        .bind(&link.to_pubkey)
        .bind(&link.to_project)
        .bind(link.status.as_str())
        .bind(link.created_ts_us)
        .execute(pool)
        .await?;
        Ok(())
    }

    /// Send a NIP-17 control message (toron-kind tags already in `tags`).
    async fn send_control(
        &self,
        sender: &Keys,
        recipient: &PublicKey,
        tags: Vec<Tag>,
    ) -> anyhow::Result<()> {
        let rumor = crate::nostr::gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::now(),
            nostr_sdk::nostr::Kind::PrivateDirectMessage,
            tags,
            "",
        );
        let wrap = crate::nostr::gift_wrap::gift_wrap(sender, recipient, rumor)
            .await
            .expect("wrap control");
        self.publish_dm(&wrap).await?;
        Ok(())
    }

    pub(crate) fn index_pool_required(&self) -> anyhow::Result<&sqlx::SqlitePool> {
        self.index_pool
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("no index pool attached (Mailbox::with_index_pool)"))
    }
}

/// i64-µs wall clock (ADR G7).
fn now_us() -> i64 {
    crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock)
}

/// A [`PolicySource`] backed by a pre-loaded snapshot of the bus's kind-30014
/// events + the index's approved AgentLinks (the projection's G14 gate).
///
/// The projection pipeline's `PolicySource` trait is synchronous (it gates a
/// per-event path that must not await), so the source is loaded **once** via
/// [`BusPolicySource::load`] and serves the frozen snapshot afterward.
pub struct BusPolicySource {
    policies: Arc<parking_lot::RwLock<HashMap<String, ContactPolicy>>>,
    approved: Arc<parking_lot::RwLock<HashSet<(String, String)>>>,
}

impl std::fmt::Debug for BusPolicySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BusPolicySource")
            .field("policies", &self.policies.read().len())
            .field("approved", &self.approved.read().len())
            .finish()
    }
}

impl Default for BusPolicySource {
    fn default() -> Self {
        Self {
            policies: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            approved: Arc::new(parking_lot::RwLock::new(HashSet::new())),
        }
    }
}

impl BusPolicySource {
    /// Load the policy snapshot for `npub` from the mailbox (kind-30014 on the
    /// bus + approved AgentLinks in the index).
    pub async fn load(mailbox: &Mailbox, npub: &PublicKey) -> Self {
        Self::load_many(mailbox, &[npub]).await
    }

    /// Load the policy snapshot for **multiple** recipients in one pass (the
    /// daemon inbound unwrap loop serves every local agent key with a single
    /// [`UnwrapPipeline`]). `load` is a specialization for one npub.
    pub async fn load_many(mailbox: &Mailbox, npubs: &[&PublicKey]) -> Self {
        let source = Self::default();
        // Per-npub policy from kind-30014 on the bus.
        for npub in npubs {
            let project = mailbox.project_for_npub(npub).await;
            if !project.is_empty() {
                let policy = mailbox.contact_policy_for(&project, npub).await;
                source.policies.write().insert(npub.to_hex(), policy);
            }
        }
        // Approved links touching any of the recipients (one query per npub,
        // unioned into the shared approved set).
        if let Some(pool) = mailbox.index_pool() {
            for npub in npubs {
                let rows = sqlx::query(
                    "SELECT from_pubkey, to_pubkey FROM contacts \
                     WHERE status = 'approved' AND (from_pubkey = ?1 OR to_pubkey = ?1)",
                )
                .bind(npub.to_hex())
                .fetch_all(pool)
                .await;
                if let Ok(rows) = rows {
                    let mut approved = source.approved.write();
                    for row in rows {
                        let a: String = row.get("from_pubkey");
                        let b: String = row.get("to_pubkey");
                        approved.insert((a.clone(), b.clone()));
                    }
                }
            }
        }
        source
    }
}

impl crate::mailbox::unwrap::PolicySource for BusPolicySource {
    fn policy_for(&self, recipient: &PublicKey) -> ContactPolicy {
        self.policies
            .read()
            .get(&recipient.to_hex())
            .copied()
            .unwrap_or(ContactPolicy::Auto)
    }

    fn is_approved_contact(&self, recipient: &PublicKey, sender: &PublicKey) -> bool {
        self.approved
            .read()
            .contains(&(recipient.to_hex(), sender.to_hex()))
            || self
                .approved
                .read()
                .contains(&(sender.to_hex(), recipient.to_hex()))
    }
}

impl Mailbox {
    /// Find the project an agent npub belongs to (first project whose
    /// kind-30013 profile is authored by `npub`). Empty when unknown.
    async fn project_for_npub(&self, npub: &PublicKey) -> String {
        // Scan known projects on disk (data_dir/*).
        let Ok(entries) = std::fs::read_dir(&self.data_dir) else {
            return String::new();
        };
        for entry in entries.flatten() {
            let slug = entry.file_name().to_string_lossy().to_string();
            let filter = kind_project_filter(toron_kind(AGENT_PROFILE), &slug);
            if let Ok(events) = collect_replay(self.bus.as_ref(), filter).await
                && events.iter().any(|e| e.pubkey == *npub)
            {
                return slug;
            }
        }
        String::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::mailbox::unwrap::PolicySource;
    use crate::nostr::identity::AgentProfile;
    use std::sync::Arc;

    const PASSPHRASE: &str = "contacts-test-pass";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path = std::env::temp_dir().join(format!(
            "toron-contacts-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir = std::env::temp_dir().join(format!(
            "toron-contacts-data-{tag}-{}",
            uuid::Uuid::new_v4()
        ));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE)
            .with_index_pool(pool);
        (mailbox, db_path, data_dir)
    }

    fn profile(name: &str) -> AgentProfile {
        AgentProfile {
            name: name.into(),
            program: Some("opencode".into()),
            model: None,
            persona: None,
            task_description: None,
        }
    }

    async fn register(mailbox: &Mailbox, project: &str, name: &str) -> PublicKey {
        let agent = mailbox
            .register_agent(project, &profile(name))
            .await
            .expect("register");
        PublicKey::from_hex(&agent.npub).expect("npub")
    }

    #[tokio::test]
    async fn test_set_policy_publishes_kind_30014() {
        let (mailbox, db_path, data_dir) = test_mailbox("policy").await;
        register(&mailbox, "demo", "BlueLake").await;

        let id = mailbox
            .set_contact_policy("demo", "BlueLake", ContactPolicy::BlockAll)
            .await
            .expect("set policy");
        assert!(!id.is_empty());

        // The policy event is on the bus (kind-30014) with dual-emit scoping.
        let filter = kind_project_filter(toron_kind(CONTACT_POLICY), "demo");
        let events = collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("collect");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind.as_u16(), CONTACT_POLICY);
        assert_eq!(events[0].content, "block_all");
        assert!(
            events[0]
                .tags
                .iter()
                .any(|t| t.as_slice() == ["t".to_string(), "demo".to_string()])
        );
        assert!(events[0]
            .tags
            .iter()
            .any(|t| t.as_slice() == ["l".to_string(), "demo".to_string(), "toron".to_string()]));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_request_respond_list_flow() {
        let (mailbox, db_path, data_dir) = test_mailbox("flow").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        let link = mailbox
            .request_contact(&RequestContact {
                project_slug: "demo".into(),
                from_agent: "BlueLake".into(),
                to_agent: "RoseDune".into(),
                to_project: None,
                reason: Some("peer".into()),
            })
            .await
            .expect("request");
        assert_eq!(link.status, LinkStatus::Pending);
        assert_eq!(link.from_pubkey, blue.to_hex());

        // Not approved yet → contacts_only would drop; is_approved_contact false.
        assert!(
            !mailbox
                .is_approved_contact(
                    &blue,
                    &mailbox
                        .agent_keys_for("demo", "RoseDune")
                        .unwrap()
                        .public_key()
                )
                .await
        );

        let _ = mailbox
            .respond_contact(&RespondContact {
                project_slug: "demo".into(),
                to_agent: "RoseDune".into(),
                from_agent: "BlueLake".into(),
                accept: true,
                from_project: None,
            })
            .await
            .expect("approve");

        let links = mailbox
            .list_contacts("demo", "RoseDune")
            .await
            .expect("list");
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].status, LinkStatus::Approved);
        assert_eq!(links[0].from_name, "BlueLake");

        // Approved → is_approved_contact true.
        let rose = mailbox
            .agent_keys_for("demo", "RoseDune")
            .unwrap()
            .public_key();
        assert!(mailbox.is_approved_contact(&blue, &rose).await);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_deny_keeps_link_unapproved() {
        let (mailbox, db_path, data_dir) = test_mailbox("deny").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;
        register(&mailbox, "demo", "RoseDune").await;

        mailbox
            .request_contact(&RequestContact {
                project_slug: "demo".into(),
                from_agent: "BlueLake".into(),
                to_agent: "RoseDune".into(),
                to_project: None,
                reason: None,
            })
            .await
            .expect("request");

        mailbox
            .respond_contact(&RespondContact {
                project_slug: "demo".into(),
                to_agent: "RoseDune".into(),
                from_agent: "BlueLake".into(),
                accept: false,
                from_project: None,
            })
            .await
            .expect("deny");

        let links = mailbox
            .list_contacts("demo", "RoseDune")
            .await
            .expect("list");
        assert_eq!(links[0].status, LinkStatus::Denied);
        let rose = mailbox
            .agent_keys_for("demo", "RoseDune")
            .unwrap()
            .public_key();
        assert!(
            !mailbox.is_approved_contact(&blue, &rose).await,
            "denied ≠ approved"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_bus_policy_source_block_all_drops() {
        let (mailbox, db_path, data_dir) = test_mailbox("source").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;
        mailbox
            .set_contact_policy("demo", "BlueLake", ContactPolicy::BlockAll)
            .await
            .expect("set");

        let source = BusPolicySource::load(&mailbox, &blue).await;
        let policy = source.policy_for(&blue);
        assert_eq!(policy, ContactPolicy::BlockAll, "kind-30014 read back");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_default_policy_is_auto() {
        let (mailbox, db_path, data_dir) = test_mailbox("default").await;
        let blue = register(&mailbox, "demo", "BlueLake").await;
        let source = BusPolicySource::load(&mailbox, &blue).await;
        assert_eq!(source.policy_for(&blue), ContactPolicy::Auto);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }
}
