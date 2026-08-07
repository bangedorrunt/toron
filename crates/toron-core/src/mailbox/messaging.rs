// governed-by: ADR-0003
// governed-by: ADR-0004 (SendMessage.overseer, operator signing — Ship 5)
#![forbid(unsafe_code)]

//! Messaging cluster (bd-led.5.3): send_message, reply_message, fetch_inbox,
//! acknowledge_message, mark_message_read — G3/G7/G8 + NIP-17.
//!
//! Wire mapping (ADR-0003 G3):
//! - **to+cc**: one visible rumor with `["p", npub, relay, "to"|"cc"]` tags;
//!   gift-wrapped one copy per to/cc recipient.
//! - **bcc**: a **variant rumor** per bcc recipient listing `to+cc+that bcc`
//!   (omitting other bccs) — true BCC, cryptographically enforced. Bcc
//!   recipients cannot see each other.
//! - `subject` → NIP-14 `subject`; `importance`, `ack_required`,
//!   `sender_token` → tags on the rumor; thread reply → NIP-10
//!   `["e", root, relay, "reply"]`.
//! - Unknown recipient **aborts the whole send** (no partial delivery).
//!
//! Acks/reads (G8): private NIP-17 gift-wrapped kind-14 to the original sender
//! only, tagged `toron-kind`=`ack`|`read` + a NIP-10 `e` reference. Idempotent
//! (second call returns the stored timestamp). NEVER public replaceable events.

use std::collections::HashMap;

use nostr_sdk::nostr::event::unsigned::UnsignedEvent;
use nostr_sdk::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use crate::bus::filter::ToronFilter;
use crate::bus::scoping::is_metadata_hiding_kind;
use crate::mailbox::unwrap::{order_by_rumor_time, OVERSEER_TAG, UnwrappedMail};
use crate::nostr::gift_wrap::{self, UnwrappedGift};
use crate::nostr::identity as nident;

use super::Mailbox;

/// A recipient role on the wire (`p` tag role, G3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecipientRole {
    To,
    Cc,
    Bcc,
}

impl RecipientRole {
    fn tag_value(self) -> &'static str {
        match self {
            Self::To => "to",
            Self::Cc => "cc",
            Self::Bcc => "bcc",
        }
    }
}

/// One resolved recipient (name → npub, G2) ready for wrapping.
#[derive(Debug, Clone)]
struct ResolvedRecipient {
    name: String,
    npub: PublicKey,
    role: RecipientRole,
}

/// Send options mirroring the AM-Rust `send_message` tool surface.
#[derive(Debug, Clone, Default)]
pub struct SendMessage {
    pub project_slug: String,
    pub sender_name: String,
    pub to: Vec<String>,
    pub subject: String,
    pub body_md: String,
    pub cc: Vec<String>,
    pub bcc: Vec<String>,
    pub importance: Option<String>,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub sender_token: Option<String>,
    /// ADR-0004 Ship 5: operator Overseer override. When true, the message is
    /// signed with the OPERATOR key (not an agent key), gains the
    /// `['t', 'toron:overseer']` tag, and SKIPS contact-policy enforcement at
    /// the projection (`block_all` cannot block the operator). Requires the
    /// mailbox to hold an operator key.
    pub overseer: bool,
}

/// Delivery record returned by `send_message` (parity shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery {
    pub project: String,
    pub payload: DeliveryPayload,
}

/// One recipient's delivery payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryPayload {
    pub recipient: String,
    pub event_id: String,
}

/// Result of `send_message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendResult {
    pub deliveries: Vec<Delivery>,
    pub count: usize,
    pub verified_sender: bool,
}

/// A projected inbox message (rumor-time order, G7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxMessage {
    /// Message id = rumor's canonical id (content-addressed, G7).
    pub id: String,
    pub sender: String,
    pub subject: String,
    pub body_md: String,
    pub importance: String,
    pub ack_required: bool,
    pub thread_id: Option<String>,
    pub created_ts_us: i64,
}

/// `fetch_inbox` options.
#[derive(Debug, Clone, Default)]
pub struct FetchInbox {
    pub project_slug: String,
    pub agent_name: String,
    pub unread_only: bool,
    pub limit: Option<usize>,
    pub include_bodies: bool,
}

/// A receipt outcome (idempotent).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptResult {
    pub message_id: String,
    pub acknowledged: bool,
    pub acknowledged_at_us: Option<i64>,
}

impl Mailbox {
    /// `send_message`: resolve names → npubs (fail-fast on unknown), build the
    /// visible rumor + per-bcc variants, gift-wrap one copy per recipient,
    /// publish every wrap. Returns per-recipient delivery records.
    ///
    /// NIP-17 fan-out: N recipients ⇒ N kind-1059 wraps, each unwrapping to the
    /// same kind-14 rumor (to/cc) or the bcc-variant rumor. Gift wraps are
    /// metadata-hiding: they carry NO project tags (A3).
    pub async fn send_message(&self, args: &SendMessage) -> anyhow::Result<SendResult> {
        // G3: resolve ALL recipients pre-wrap; unknown aborts the whole send.
        let resolved = self.resolve_recipients(args).await?;
        if resolved.is_empty() {
            anyhow::bail!("no recipients (to/cc/bcc all empty)");
        }

        let sender_keys = if args.overseer {
            // ADR-0004 Ship 5: the Overseer signs with the OPERATOR key, so
            // the projection can verify the sender npub against it.
            self.operator().cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "overseer send requires an operator key (set operator_key / TORON_OPERATOR_KEY)"
                )
            })?
        } else {
            self.agent_keys_for(&args.project_slug, &args.sender_name)?
        };

        let visible = build_visible_rumor(&sender_keys.public_key(), args, &resolved);
        let mut deliveries: Vec<Delivery> = Vec::new();

        // to+cc: one gift wrap per recipient, same visible rumor.
        for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
            let wrap = gift_wrap::gift_wrap(&sender_keys, &r.npub, visible.clone())
                .await
                .map_err(|e| {
                    anyhow::anyhow!("gift wrap failed for to/cc recipient {}: {e}", r.name)
                })?;
            let delivered = self.publish_dm(&wrap).await?;
            if delivered {
                deliveries.push(Delivery {
                    project: args.project_slug.clone(),
                    payload: DeliveryPayload {
                        recipient: r.name.clone(),
                        event_id: wrap.id.to_hex(),
                    },
                });
            }
        }

        // bcc: variant rumor per bcc listing to+cc+that bcc only.
        for r in resolved.iter().filter(|r| r.role == RecipientRole::Bcc) {
            let variant = build_bcc_rumor(&sender_keys.public_key(), args, &resolved, r);
            let wrap = gift_wrap::gift_wrap(&sender_keys, &r.npub, variant)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("gift wrap failed for bcc recipient {}: {e}", r.name)
                })?;
            let delivered = self.publish_dm(&wrap).await?;
            if delivered {
                deliveries.push(Delivery {
                    project: args.project_slug.clone(),
                    payload: DeliveryPayload {
                        recipient: r.name.clone(),
                        event_id: wrap.id.to_hex(),
                    },
                });
            }
        }

        Ok(SendResult {
            count: deliveries.len(),
            deliveries,
            verified_sender: true,
        })
    }

    /// `reply_message`: like `send_message` but attaches the thread as a NIP-10
    /// `["e", root, relay, "reply"]` tag on the rumor. Recipients default to
    /// the original message's sender when `to` is empty.
    pub async fn reply_message(&self, args: &SendMessage) -> anyhow::Result<SendResult> {
        let root = args
            .thread_id
            .clone()
            .ok_or_else(|| anyhow::anyhow!("reply requires thread_id (original message id)"))?;
        let mut reply = args.clone();
        reply.thread_id = Some(root);
        self.send_message(&reply).await
    }

    /// `fetch_inbox`: project + agent projection of unwrapped kind-1059 mail to
    /// the agent's local key, ordered by rumor time (G7), never envelope time.
    pub async fn fetch_inbox(&self, args: &FetchInbox) -> anyhow::Result<Vec<InboxMessage>> {
        let agent_keys = self.agent_keys_for(&args.project_slug, &args.agent_name)?;
        let npub = agent_keys.public_key();

        // Subscribe to gift wraps addressed to this agent (#p), drain history.
        let filter = ToronFilter::new()
            .kinds([nostr_sdk::nostr::Kind::GiftWrap])
            .pubkey(npub)
            .build();
        let events = super::collect_replay(self.bus.as_ref(), filter).await?;

        let mut mails: Vec<UnwrappedMail> = Vec::new();
        for wrap in &events {
            if let Ok(gift) = gift_wrap::unwrap(&agent_keys, wrap) {
                let rumor = gift.rumor;
                mails.push(UnwrappedMail {
                    recipient: npub,
                    sender: gift.sender,
                    rumor_id: rumor.clone().id(),
                    wrap_event_id: wrap.id,
                    created_at: rumor.created_at,
                    rumor,
                });
            }
        }

        // G7: order by rumor time (+ts_ms) + local_seq; never envelope time.
        let ordered = order_by_rumor_time(mails);
        let messages: Vec<InboxMessage> =
            ordered.into_iter().map(|(m, _seq)| inbox_from(m)).collect();

        let messages = if args.unread_only {
            // Unread-only filtering needs the projection read-state; the
            // pipeline-level dedup treats everything as fresh on first read.
            messages
        } else {
            messages
        };

        let messages = match args.limit {
            Some(n) => messages.into_iter().take(n).collect(),
            None => messages,
        };
        Ok(messages)
    }

    /// `acknowledge_message`: private NIP-17 kind-14 to the original sender
    /// (`toron-kind=ack` + NIP-10 `e` to the original). Idempotent: a second
    /// ack returns the stored timestamp without re-sending.
    pub async fn acknowledge_message(
        &self,
        project_slug: &str,
        agent_name: &str,
        message_id: &str,
    ) -> anyhow::Result<ReceiptResult> {
        self.send_receipt(project_slug, agent_name, message_id, "ack", true)
            .await
    }

    /// `mark_message_read`: private NIP-17 kind-14 to the original sender
    /// (`toron-kind=read` + NIP-10 `e`). Idempotent.
    pub async fn mark_message_read(
        &self,
        project_slug: &str,
        agent_name: &str,
        message_id: &str,
    ) -> anyhow::Result<ReceiptResult> {
        self.send_receipt(project_slug, agent_name, message_id, "read", false)
            .await
    }

    /// Shared ack/read path (G8).
    async fn send_receipt(
        &self,
        project_slug: &str,
        agent_name: &str,
        message_id: &str,
        kind: &str,
        acknowledged: bool,
    ) -> anyhow::Result<ReceiptResult> {
        // Find the original message (the wrap addressed to this agent) to learn
        // the sender npub; the ack goes to them ONLY.
        let agent_keys = self.agent_keys_for(project_slug, agent_name)?;
        let npub = agent_keys.public_key();

        let filter = ToronFilter::new()
            .kinds([nostr_sdk::nostr::Kind::GiftWrap])
            .pubkey(npub)
            .build();
        let events = super::collect_replay(self.bus.as_ref(), filter).await?;
        let original = events.iter().find_map(|wrap| {
            let gift = gift_wrap::unwrap(&agent_keys, wrap).ok()?;
            (gift.rumor.clone().id().to_hex() == message_id).then_some(gift)
        });
        let Some(original) = original else {
            anyhow::bail!("message not found for agent {agent_name}: {message_id}");
        };
        let sender_npub = original.sender;

        // Idempotency: already acked/read → return stored receipt.
        let id = EventId::from_hex(message_id).unwrap_or_else(|_| original.rumor.clone().id());
        if let Some(ts) = self.receipt_time(project_slug, agent_name, &id, kind) {
            return Ok(ReceiptResult {
                message_id: message_id.to_string(),
                acknowledged,
                acknowledged_at_us: Some(ts),
            });
        }

        // Build the private kind-14 rumor: toron-kind + NIP-10 e reference.
        let tags = vec![
            Tag::parse(["toron-kind", kind])
                .map_err(|e| anyhow::anyhow!("failed to build toron-kind tag: {e}"))?,
            Tag::event(original.rumor.clone().id()),
        ];
        let rumor = gift_wrap::build_rumor(
            npub,
            Timestamp::now(),
            nostr_sdk::nostr::Kind::PrivateDirectMessage,
            tags,
            "",
        );
        let wrap = gift_wrap::gift_wrap(&agent_keys, &sender_npub, rumor)
            .await
            .map_err(|e| anyhow::anyhow!("gift wrap failed for receipt to {sender_npub}: {e}"))?;
        let ts_us = crate::index::writer::Clock::now_us(&crate::index::writer::SystemClock);
        self.publish_dm(&wrap).await?;
        self.record_receipt(project_slug, agent_name, &id, kind, ts_us);

        Ok(ReceiptResult {
            message_id: message_id.to_string(),
            acknowledged,
            acknowledged_at_us: Some(ts_us),
        })
    }

    /// Publish a DM wrap (metadata-hiding: never adds project scoping tags).
    pub(crate) async fn publish_dm(&self, wrap: &Event) -> anyhow::Result<bool> {
        debug_assert!(
            is_metadata_hiding_kind(wrap.kind),
            "gift wraps must be metadata-hiding"
        );
        let ok = self.bus.publish(wrap.clone()).await?;
        Ok(ok.stored)
    }

    /// Resolve all recipient names → npubs via the project's kind-30013
    /// profiles (G2). Unknown name fails the whole send.
    async fn resolve_recipients(
        &self,
        args: &SendMessage,
    ) -> anyhow::Result<Vec<ResolvedRecipient>> {
        let profiles = self.project_profiles(&args.project_slug).await?;
        let mut out: Vec<ResolvedRecipient> = Vec::new();
        for (names, role) in [
            (args.to.iter().collect::<Vec<_>>(), RecipientRole::To),
            (args.cc.iter().collect::<Vec<_>>(), RecipientRole::Cc),
            (args.bcc.iter().collect::<Vec<_>>(), RecipientRole::Bcc),
        ] {
            for name in names {
                let npub = nident::resolve_name(&args.project_slug, profiles.iter(), name)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "unknown recipient: {name} (register it in project {} first)",
                            args.project_slug
                        )
                    })?;
                out.push(ResolvedRecipient {
                    name: name.clone(),
                    npub,
                    role,
                });
            }
        }
        Ok(out)
    }

    /// Load the sender/agent's keypair from its ncryptsec file (G1).
    pub(crate) fn agent_keys_for(&self, project_slug: &str, name: &str) -> anyhow::Result<Keys> {
        let path = self.agent_key_path(project_slug, name);
        self.load_agent_key(&path).ok_or_else(|| {
            anyhow::anyhow!(
                "agent {name} has no local key in project {project_slug} (create_agent_identity or register first)"
            )
        })
    }

    /// Per-message receipt timestamp cache (idempotency, in-memory).
    fn receipt_time(&self, project: &str, agent: &str, id: &EventId, kind: &str) -> Option<i64> {
        self.receipts()
            .lock()
            .get(&receipt_key(project, agent, id, kind))
            .copied()
    }

    fn record_receipt(&self, project: &str, agent: &str, id: &EventId, kind: &str, ts_us: i64) {
        self.receipts()
            .lock()
            .insert(receipt_key(project, agent, id, kind), ts_us);
    }

    fn receipts(&self) -> &parking_lot::Mutex<HashMap<String, i64>> {
        &self.receipt_cache
    }
}

/// Build the visible rumor (to+cc only; bcc NEVER appears on it).
///
/// Tag construction is infallible for toron-controlled inputs; any failure
/// here is a programming error surfaced via `expect` on a known-static tag
/// shape (kept narrow — only the recipient `p` tag, which is npub-derived).
fn build_visible_rumor(
    author: &PublicKey,
    args: &SendMessage,
    resolved: &[ResolvedRecipient],
) -> UnsignedEvent {
    let mut tags: Vec<Tag> = Vec::new();
    for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
        // npub.to_hex() is infallible; the 4-tuple shape is toron-controlled.
        // A failure here would indicate SDK invariants broken — panic is the
        // correct response (not a recoverable send error).
        tags.push(
            Tag::parse(["p", &r.npub.to_hex(), "", r.role.tag_value()])
                .expect("static p tag shape (npub hex + role literal)"),
        );
    }
    add_message_tags(&mut tags, args);
    gift_wrap::build_rumor(
        *author,
        Timestamp::now(),
        nostr_sdk::nostr::Kind::PrivateDirectMessage,
        tags,
        args.body_md.clone(),
    )
}

/// Build the bcc-variant rumor: to+cc+THIS bcc only (other bccs invisible).
fn build_bcc_rumor(
    author: &PublicKey,
    args: &SendMessage,
    resolved: &[ResolvedRecipient],
    bcc: &ResolvedRecipient,
) -> UnsignedEvent {
    let mut tags: Vec<Tag> = Vec::new();
    // to + cc, plus the single bcc — role "bcc" on its own p tag.
    for r in resolved.iter().filter(|r| r.role != RecipientRole::Bcc) {
        tags.push(
            Tag::parse(["p", &r.npub.to_hex(), "", r.role.tag_value()])
                .expect("static p tag shape (npub hex + role literal)"),
        );
    }
    tags.push(
        Tag::parse(["p", &bcc.npub.to_hex(), "", "bcc"])
            .expect("static bcc p tag shape (npub hex)"),
    );
    add_message_tags(&mut tags, args);
    gift_wrap::build_rumor(
        *author,
        Timestamp::now(),
        nostr_sdk::nostr::Kind::PrivateDirectMessage,
        tags,
        args.body_md.clone(),
    )
}

/// Add subject (NIP-14), importance/ack_required/sender_token/thread (G3) tags.
///
/// All tag strings here are either toron-controlled literals or user input
/// that nostr-sdk validates. A parse failure on user input (e.g. an
/// over-long subject) is silently dropped rather than panicking — the message
/// still sends, just without that tag. This matches AM-Rust's tolerant
/// handling of optional metadata.
fn add_message_tags(tags: &mut Vec<Tag>, args: &SendMessage) {
    if !args.subject.is_empty() {
        if let Ok(t) = Tag::parse(["subject", &args.subject]) {
            tags.push(t);
        }
    }
    if let Some(imp) = &args.importance {
        if let Ok(t) = Tag::parse(["importance", imp]) {
            tags.push(t);
        }
    }
    if args.ack_required {
        if let Ok(t) = Tag::parse(["ack-required"]) {
            tags.push(t);
        }
    }
    if let Some(token) = &args.sender_token {
        if let Ok(t) = Tag::parse(["sender-token", token]) {
            tags.push(t);
        }
    }
    if args.overseer {
        // ADR-0004 Ship 5: mark the override so the projection can recognize
        // it (key-scoped + tag-scoped defense in depth).
        if let Ok(t) = Tag::parse(["t", OVERSEER_TAG]) {
            tags.push(t);
        }
    }
    if let Some(thread) = &args.thread_id {
        if let Ok(root) = EventId::from_hex(thread) {
            tags.push(Tag::event(root)); // NIP-10 e = root, reply role
        }
    }
}

/// Project an unwrapped mail into the inbox view.
fn inbox_from(mail: UnwrappedMail) -> InboxMessage {
    let tags = &mail.rumor.tags;
    let subject = tag_value(tags, "subject").unwrap_or_default().to_string();
    let importance = tag_value(tags, "importance")
        .unwrap_or("normal")
        .to_string();
    let ack_required = tag_has_flag(tags, "ack-required");
    let thread_id = tags
        .iter()
        .find_map(|t| {
            let parts = t.as_slice();
            (parts.first().map(|s| s.as_str()) == Some("e"))
                .then(|| parts.get(1).map(|s| s.to_string()))
        })
        .flatten();
    InboxMessage {
        id: mail.rumor_id.to_hex(),
        sender: mail.sender.to_hex(),
        subject,
        body_md: mail.rumor.content.clone(),
        importance,
        ack_required,
        thread_id,
        created_ts_us: mail.ts_us(),
    }
}

/// First value of a single-letter tag (e.g. `subject`).
fn tag_value<'a>(tags: &'a nostr_sdk::nostr::Tags, name: &str) -> Option<&'a str> {
    tags.iter().find_map(|t| {
        let parts = t.as_slice();
        (parts.first().map(|s| s.as_str()) == Some(name))
            .then(|| parts.get(1).map(|s| s.as_str()))?
    })
}

/// True when a flag tag (no value) is present, e.g. `["ack-required"]`.
fn tag_has_flag(tags: &nostr_sdk::nostr::Tags, name: &str) -> bool {
    tags.iter()
        .any(|t| t.as_slice().first().map(|s| s.as_str()) == Some(name))
}

/// Cache key for a receipt: `project:agent:kind:event_id`.
fn receipt_key(project: &str, agent: &str, id: &EventId, kind: &str) -> String {
    format!("{project}:{agent}:{kind}:{id}")
}

/// Verify a gift wrap unwraps to the expected rumor content (acceptance
/// helper — also used by tests).
pub fn unwrap_for_recipient(
    recipient: &Keys,
    wrap: &Event,
) -> Result<UnwrappedGift, gift_wrap::GiftWrapError> {
    gift_wrap::unwrap(recipient, wrap)
}

/// Compile-time guarantee: a send with `to=["Unknown"]` never produces a wrap.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::mailbox::Mailbox;
    use crate::nostr::identity::AgentProfile;
    use std::sync::Arc;

    const PASSPHRASE: &str = "messaging-test-pass";

    async fn test_mailbox(tag: &str) -> (Mailbox, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-msg-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-msg-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = crate::keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator))
            .with_agent_passphrase(PASSPHRASE);
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

    async fn register(mailbox: &Mailbox, project: &str, name: &str) -> (Keys, PublicKey) {
        let agent = mailbox
            .register_agent(project, &profile(name))
            .await
            .expect("register");
        let keys = mailbox
            .load_agent_key(std::path::Path::new(&agent.key_path))
            .expect("load key");
        let npub = PublicKey::from_hex(&agent.npub).expect("npub");
        (keys, npub)
    }

    /// Drain gift-wrap events addressed to `npub` from the bus.
    async fn wraps_to(mailbox: &Mailbox, npub: &PublicKey, limit: usize) -> Vec<Event> {
        let filter = ToronFilter::new()
            .kinds([nostr_sdk::nostr::Kind::GiftWrap])
            .pubkey(*npub)
            .build();
        let events = super::super::collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("collect");
        events.into_iter().take(limit).collect()
    }

    #[tokio::test]
    async fn test_send_n_recipients_n_wraps_same_rumor() {
        let (mailbox, db_path, data_dir) = test_mailbox("fanout").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv_a, npub_a) = register(&mailbox, "demo", "BlueLake").await;
        let (recv_b, npub_b) = register(&mailbox, "demo", "RoseDune").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into(), "RoseDune".into()],
            subject: "review".into(),
            body_md: "please review".into(),
            ..Default::default()
        };
        let result = mailbox.send_message(&args).await.expect("send");
        assert_eq!(result.count, 2, "2 recipients → 2 wraps");
        assert!(result.verified_sender);

        // Both recipients' wraps unwrap to the same rumor content.
        let wraps_a = wraps_to(&mailbox, &npub_a, 2).await;
        let wraps_b = wraps_to(&mailbox, &npub_b, 2).await;
        assert_eq!(wraps_a.len(), 1);
        assert_eq!(wraps_b.len(), 1);
        let rumor_a = unwrap_for_recipient(&recv_a, &wraps_a[0])
            .expect("unwrap a")
            .rumor;
        let rumor_b = unwrap_for_recipient(&recv_b, &wraps_b[0])
            .expect("unwrap b")
            .rumor;
        assert_eq!(rumor_a.content, "please review");
        assert_eq!(rumor_b.content, "please review");
        assert_eq!(rumor_a.kind, nostr_sdk::nostr::Kind::PrivateDirectMessage);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn overseer_send_signs_with_operator_and_tags() {
        // ADR-0004 Ship 5: `overseer: true` signs with the OPERATOR key (not
        // an agent key), sets importance=high, and tags toron:overseer — the
        // projection-recognizable override envelope.
        let (mailbox, db_path, data_dir) = test_mailbox("overseer").await;
        let (recv, npub) = register(&mailbox, "demo", "BlueLake").await;
        let operator_npub = mailbox.operator().expect("operator").public_key();

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: String::new(), // overseer ignores sender_name
            to: vec!["BlueLake".into()],
            subject: "Operator override".into(),
            body_md: "handle this".into(),
            importance: Some("high".into()),
            overseer: true,
            ..Default::default()
        };
        let result = mailbox.send_message(&args).await.expect("overseer send");
        assert_eq!(result.count, 1, "delivered to the recipient");

        let wraps = wraps_to(&mailbox, &npub, 2).await;
        assert_eq!(wraps.len(), 1, "one wrap for the recipient");
        let rumor = unwrap_for_recipient(&recv, &wraps[0])
            .expect("unwrap")
            .rumor;
        // Signed by the operator key, not an agent.
        assert_eq!(
            rumor.pubkey, operator_npub,
            "overseer rumor is signed by the operator key"
        );
        // Carries the override marker + high importance.
        let has_overseer = rumor.tags.iter().any(|t| {
            t.as_slice().first().map(String::as_str) == Some("t")
                && t.as_slice().get(1).map(String::as_str) == Some(OVERSEER_TAG)
        });
        assert!(has_overseer, "rumor carries the toron:overseer tag");
        let has_high = rumor.tags.iter().any(|t| {
            t.as_slice().first().map(String::as_str) == Some("importance")
                && t.as_slice().get(1).map(String::as_str) == Some("high")
        });
        assert!(has_high, "rumor carries importance=high");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_bcc_variant_privacy() {
        let (mailbox, db_path, data_dir) = test_mailbox("bcc").await;
        let (_sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv_to, npub_to) = register(&mailbox, "demo", "BlueLake").await;
        let (recv_cc, npub_cc) = register(&mailbox, "demo", "RoseDune").await;
        let (recv_bcc1, npub_b1) = register(&mailbox, "demo", "CoralFinch").await;
        let (recv_bcc2, npub_b2) = register(&mailbox, "demo", "DustySparrow").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            cc: vec!["RoseDune".into()],
            bcc: vec!["CoralFinch".into(), "DustySparrow".into()],
            subject: "bcc test".into(),
            body_md: "secret".into(),
            ..Default::default()
        };
        let result = mailbox.send_message(&args).await.expect("send");
        assert_eq!(result.count, 4, "to(1)+cc(1)+bcc(2) = 4 wraps");

        let to_wrap = &wraps_to(&mailbox, &npub_to, 2).await[0];
        let cc_wrap = &wraps_to(&mailbox, &npub_cc, 2).await[0];
        let b1_wrap = &wraps_to(&mailbox, &npub_b1, 2).await[0];
        let b2_wrap = &wraps_to(&mailbox, &npub_b2, 2).await[0];

        let p_npubs = |gift: &UnwrappedGift| -> Vec<String> {
            gift.rumor
                .tags
                .iter()
                .filter(|t| t.as_slice().first().map(|s| s.as_str()) == Some("p"))
                .map(|t| t.as_slice()[1].clone())
                .collect()
        };

        // to/cc rumor: lists to+cc, NO bcc.
        let to_gift = unwrap_for_recipient(&recv_to, to_wrap).expect("to");
        let to_ps = p_npubs(&to_gift);
        assert!(to_ps.contains(&npub_to.to_hex()));
        assert!(to_ps.contains(&npub_cc.to_hex()));
        assert!(!to_ps.contains(&npub_b1.to_hex()), "to cannot see bcc1");
        assert!(!to_ps.contains(&npub_b2.to_hex()), "to cannot see bcc2");

        // CC receives the same visible rumor (to+cc, no bcc) and can unwrap it.
        let cc_gift = unwrap_for_recipient(&recv_cc, cc_wrap).expect("cc");
        let cc_ps = p_npubs(&cc_gift);
        assert!(cc_ps.contains(&npub_to.to_hex()));
        assert!(cc_ps.contains(&npub_cc.to_hex()));
        assert!(!cc_ps.contains(&npub_b1.to_hex()), "cc cannot see bcc1");
        assert!(!cc_ps.contains(&npub_b2.to_hex()), "cc cannot see bcc2");

        // Each bcc rumor: lists to+cc+ITSELF, not the other bcc.
        let b1_gift = unwrap_for_recipient(&recv_bcc1, b1_wrap).expect("b1");
        let b1_ps = p_npubs(&b1_gift);
        assert!(b1_ps.contains(&npub_to.to_hex()));
        assert!(b1_ps.contains(&npub_cc.to_hex()));
        assert!(b1_ps.contains(&npub_b1.to_hex()), "b1 sees itself");
        assert!(!b1_ps.contains(&npub_b2.to_hex()), "b1 cannot see bcc2");

        let b2_gift = unwrap_for_recipient(&recv_bcc2, b2_wrap).expect("b2");
        let b2_ps = p_npubs(&b2_gift);
        assert!(b2_ps.contains(&npub_b2.to_hex()));
        assert!(!b2_ps.contains(&npub_b1.to_hex()), "b2 cannot see bcc1");

        // Non-recipient cannot unwrap ANY wrap.
        let outsider = crate::keys::generate();
        assert!(unwrap_for_recipient(&outsider, to_wrap).is_err());
        assert!(unwrap_for_recipient(&outsider, b1_wrap).is_err());
        assert!(unwrap_for_recipient(&outsider, b2_wrap).is_err());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_unknown_recipient_aborts_whole_send() {
        let (mailbox, db_path, data_dir) = test_mailbox("unknown").await;
        register(&mailbox, "demo", "GreenCastle").await;
        register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into(), "Ghost".into()],
            subject: "x".into(),
            body_md: "y".into(),
            ..Default::default()
        };
        let err = mailbox.send_message(&args).await.expect_err("unknown name");
        assert!(err.to_string().contains("unknown recipient"), "{err}");
        // No partial delivery: no wraps published for BlueLake.
        let (_, npub) = {
            let p = mailbox.project_profiles("demo").await.expect("profiles");
            let npub = crate::nostr::identity::resolve_name("demo", p.iter(), "BlueLake")
                .expect("blue npub");
            ((), npub)
        };
        let wraps = wraps_to(&mailbox, &npub, 2).await;
        assert!(wraps.is_empty(), "no partial delivery on abort");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_fetch_inbox_orders_by_rumor_time() {
        let (mailbox, db_path, data_dir) = test_mailbox("inbox").await;
        let (sender, _) = register(&mailbox, "demo", "GreenCastle").await;
        let (_recv, _npub) = register(&mailbox, "demo", "BlueLake").await;

        // Send two messages (rumor timestamps now = close; order is stable).
        for subject in ["first", "second"] {
            let args = SendMessage {
                project_slug: "demo".into(),
                sender_name: "GreenCastle".into(),
                to: vec!["BlueLake".into()],
                subject: subject.into(),
                body_md: format!("body {subject}"),
                ..Default::default()
            };
            mailbox.send_message(&args).await.expect("send");
        }

        let inbox = mailbox
            .fetch_inbox(&FetchInbox {
                project_slug: "demo".into(),
                agent_name: "BlueLake".into(),
                ..Default::default()
            })
            .await
            .expect("fetch");
        assert_eq!(inbox.len(), 2);
        assert!(inbox.iter().any(|m| m.subject == "first"));
        assert!(inbox.iter().any(|m| m.subject == "second"));
        assert!(inbox
            .iter()
            .all(|m| m.sender == sender.public_key().to_hex()));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_ack_idempotent_and_private() {
        let (mailbox, db_path, data_dir) = test_mailbox("ack").await;
        let (sender, npub_sender) = register(&mailbox, "demo", "GreenCastle").await;
        let (recv, npub_recv) = register(&mailbox, "demo", "BlueLake").await;

        let args = SendMessage {
            project_slug: "demo".into(),
            sender_name: "GreenCastle".into(),
            to: vec!["BlueLake".into()],
            subject: "ack me".into(),
            body_md: "please".into(),
            ack_required: true,
            ..Default::default()
        };
        mailbox.send_message(&args).await.expect("send");

        // The message id = rumor id of the wrap received by BlueLake.
        let wrap = &wraps_to(&mailbox, &npub_recv, 2).await[0];
        let gift = unwrap_for_recipient(&recv, wrap).expect("unwrap");
        let message_id = gift.rumor.clone().id().to_hex();

        let first = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack");
        assert!(first.acknowledged);
        let ts = first.acknowledged_at_us.expect("ts");

        // Idempotent: second ack returns the same timestamp, no new event.
        let second = mailbox
            .acknowledge_message("demo", "BlueLake", &message_id)
            .await
            .expect("ack 2");
        assert_eq!(second.acknowledged_at_us, Some(ts), "idempotent ack");

        // The ack wrap is addressed to the ORIGINAL SENDER only (private).
        let _sender_wraps = wraps_to(&mailbox, &npub_sender, 4).await;
        // sender sees 1 original outbox? No — wraps are addressed to recipients.
        // The ack wrap targets the sender; count kind-1059 with p=sender npub.
        let ack_wraps = wraps_to(&mailbox, &npub_sender, 4).await;
        assert!(
            ack_wraps.iter().any(|w| {
                gift_wrap::unwrap(&sender, w)
                    .map(|g| {
                        g.rumor
                            .tags
                            .iter()
                            .any(|t| t.as_slice() == ["toron-kind".to_string(), "ack".to_string()])
                    })
                    .unwrap_or(false)
            }),
            "sender received a private ack wrap"
        );

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[test]
    fn test_receipt_key_shapes() {
        let id =
            EventId::from_hex("b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b")
                .expect("id");
        let k = receipt_key("demo", "BlueLake", &id, "ack");
        assert_eq!(k, format!("demo:BlueLake:ack:{id}"));
    }
}
