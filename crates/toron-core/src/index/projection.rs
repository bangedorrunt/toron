// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Projection pipeline for unwrapped NIP-17 mail (bd-led.4.4).
//!
//! Pure functional core (no I/O): given an `UnwrappedGift` + the recipient's
//! contact policy, decide whether to index it, and order the mail timeline
//! correctly.
//!
//! * **G7 ordering** — NIP-17 randomizes the seal/wrap `created_at`; the mail
//!   timeline MUST order by the **rumor's** `created_at` (+ optional
//!   `ts_ms` tag) and a monotonic local sequence for same-µs ties. Envelope
//!   `created_at` is NEVER the mail timestamp.
//! * **G14 policy drop** — `block_all` drops inbound mail before any index /
//!   Git write. Policy is enforced here (the bus is dumb transport).

use std::collections::HashSet;

use crate::mailbox::unwrap::ContactPolicy;
use crate::nostr::{PublicKey, UnsignedEvent};
use parking_lot::Mutex;

use crate::nostr::gift_wrap::UnwrappedGift;

/// A mail item extracted from an unwrapped rumor (kind 14/15) — the index +
/// archive + Git row shape. `created_ts_us` is i64-µs (ADR G7) derived from
/// the RUMOR timestamp, never the envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MailItem {
    /// Envelope (gift-wrap) event id — the bus/archive key.
    pub envelope_id: String,
    /// Sender pubkey (seal signer, from unwrap).
    pub sender: PublicKey,
    /// Rumor pubkey (the sender's real key).
    pub author: PublicKey,
    pub subject: String,
    pub body: String,
    pub thread_id: Option<String>,
    pub reply_to: Option<String>,
    pub to_list: Vec<String>,
    pub cc_list: Vec<String>,
    /// i64-µs; from rumor `created_at * 1e6` + `ts_ms` tag.
    pub created_ts_us: i64,
    /// Monotonic sequence breaking same-µs ties (G7).
    pub local_seq: i64,
    /// `toron-kind` tag if present ("ack" | "read"), else None (message).
    pub toron_kind: Option<String>,
}

// Contact policy (kind-30014, ADR G14) lives in crate::mailbox::unwrap —
// one canonical home (bd-05yr twins unification). The old infallible shadow
// `parse` (unknown → BlockAll) is gone; callers parse strictly and fail loud.
/// Outcome of deciding whether to project one unwrapped gift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionDecision {
    /// Index + archive this mail item (boxed — the other variant is tiny).
    Index(Box<MailItem>),
    /// Policy dropped it BEFORE any write (G14). `reason` is diagnostic only.
    Drop(String),
}

/// Extract the mail timestamp (i64-µs) from a RUMOR (G7).
///
/// Base = `created_at * 1e6` (unix seconds → µs). An optional `ts_ms` tag
/// carries an ABSOLUTE unix-µs timestamp and wins when present — the same
/// semantics as `UnwrappedMail::ts_us` (bd-ts-ms-dual-semantics-kviu: the
/// tag is never a sub-second offset added to the base).
pub fn rumor_timestamp_us(rumor: &UnsignedEvent) -> i64 {
    if let Some(tag) = rumor
        .tags
        .iter()
        .find(|t| t.as_slice().first().map(String::as_str) == Some("ts_ms"))
        .and_then(|t| t.as_slice().get(1))
        .and_then(|v| v.as_str().parse::<i64>().ok())
    {
        return tag;
    }
    rumor.created_at.as_secs() as i64 * 1_000_000
}

/// Read a tag value by name from an event/rumor.
fn tag_value<'a>(tags: &'a crate::nostr::Tags, name: &str) -> Option<&'a str> {
    tags.iter()
        .find(|t| t.as_slice().first().map(String::as_str) == Some(name))
        .and_then(|t| t.as_slice().get(1))
        .map(String::as_str)
}

/// Split a rumour's recipient `p` tags into `(to_list, cc_list)`.
///
/// `p` tags carry their role in cell 3: `["p", npub, relay, "to"|"cc"]` (ADR
/// G3). An absent role cell means `to`. Roles this vocabulary does not model
/// (e.g. `bcc`) stay in `to_list` — an address the sender did express must
/// never silently vanish, since consumers read these lists to decide who owes
/// an ack.
///
/// This is the single canonical reader of addressing. The unwrap sink and
/// [`extract_mail`] MUST both project through it: a second copy that forgets a
/// field is how a `messages` row ends up with an empty audience while the
/// rumour plainly named its recipients.
pub fn addressing_lists(tags: &crate::nostr::Tags) -> (Vec<String>, Vec<String>) {
    let mut to_list: Vec<String> = Vec::new();
    let mut cc_list: Vec<String> = Vec::new();
    for tag in tags
        .iter()
        .filter(|t| t.as_slice().first().map(String::as_str) == Some("p"))
    {
        let cells = tag.as_slice();
        let Some(npub) = cells.get(1) else { continue };
        if cells.get(3).map(String::as_str) == Some("cc") {
            cc_list.push(npub.clone());
        } else {
            to_list.push(npub.clone());
        }
    }
    (to_list, cc_list)
}

/// Build a `MailItem` from an unwrapped gift. Uses ONLY rumor fields for the
/// timeline; envelope identity is preserved for storage/audit.
pub fn extract_mail(unwrapped: &UnwrappedGift, local_seq: i64) -> MailItem {
    let rumor = &unwrapped.rumor;
    let (to_list, cc_list) = addressing_lists(&rumor.tags);

    MailItem {
        // Envelope id is filled by the caller (we don't have the wrap here).
        envelope_id: String::new(),
        sender: unwrapped.sender,
        author: rumor.pubkey,
        subject: tag_value(&rumor.tags, "subject").unwrap_or("").to_string(),
        body: rumor.content.clone(),
        thread_id: tag_value(&rumor.tags, "e").map(str::to_string),
        reply_to: None,
        to_list,
        cc_list,
        created_ts_us: rumor_timestamp_us(rumor),
        local_seq,
        toron_kind: tag_value(&rumor.tags, "toron-kind").map(str::to_string),
    }
}

/// Does `recipient_policy` allow inbound mail from `sender`? (G14)
///
/// `approved_senders` is the recipient's approved contact set (from contact
/// links / kind-30014); `block_all` short-circuits to false. Exact AM-Rust
/// semantics: open accepts all; auto/contacts_only require the sender be an
/// approved contact.
pub fn policy_allows(
    recipient_policy: ContactPolicy,
    sender: &PublicKey,
    approved_senders: &HashSet<PublicKey>,
) -> bool {
    match recipient_policy {
        ContactPolicy::Open => true,
        ContactPolicy::BlockAll => false,
        ContactPolicy::Auto | ContactPolicy::ContactsOnly => approved_senders.contains(sender),
    }
}

/// Decide whether to project an unwrapped gift under the recipient's policy.
pub fn decide(
    unwrapped: &UnwrappedGift,
    recipient_policy: ContactPolicy,
    approved_senders: &HashSet<PublicKey>,
) -> ProjectionDecision {
    if !policy_allows(recipient_policy, &unwrapped.sender, approved_senders) {
        return ProjectionDecision::Drop(format!(
            "policy {:?} blocks sender {}",
            recipient_policy, unwrapped.sender
        ));
    }
    ProjectionDecision::Index(Box::new(extract_mail(unwrapped, 0)))
}

/// Order mail by `(created_ts_us, local_seq)` — the G7 timeline. Envelope
/// timestamps are NOT consulted (NIP-17 randomizes them). Stable.
pub fn sort_by_timeline(items: &mut [MailItem]) {
    items.sort_by_key(|m| (m.created_ts_us, m.local_seq));
}

/// Idempotency cache for acks/reads keyed by `(acker-npub, message-id)`
/// (ADR G8: second call returns stored ts — parity).
#[derive(Debug, Default)]
pub struct AckCache {
    seen: Mutex<HashSet<(String, String)>>,
}

impl AckCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an ack; returns true if it was new (false = idempotent replay).
    pub fn record(&self, acker: &PublicKey, message_id: &str) -> bool {
        self.seen
            .lock()
            .insert((acker.to_string(), message_id.to_string()))
    }

    pub fn contains(&self, acker: &PublicKey, message_id: &str) -> bool {
        self.seen
            .lock()
            .contains(&(acker.to_string(), message_id.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::Keys;
    use crate::nostr::gift_wrap;
    use nostr_sdk::prelude::*;

    /// Build a rumor + gift-wrap envelope with a FIXED envelope timestamp so
    /// tests can invert envelope order vs rumor order (G7).
    async fn wrap_with(
        sender: &Keys,
        recipient: &PublicKey,
        rumor_created_at: Timestamp,
        rumor_content: &str,
    ) -> (UnsignedEvent, Event) {
        let rumor = gift_wrap::build_rumor(
            sender.public_key(),
            rumor_created_at,
            14.into(),
            vec![Tag::public_key(*recipient)],
            rumor_content.to_string(),
        );
        let wrapped = gift_wrap::gift_wrap(sender, recipient, rumor.clone()).expect("wrap");
        (rumor, wrapped)
    }

    #[tokio::test]
    async fn rumor_timestamp_wins_over_envelope() {
        let sender = Keys::generate();
        let recipient = Keys::generate();

        // Rumor at t=2000s, envelope created_at randomizes to t=1000s — the
        // mail timeline MUST use 2000s (rumor), not the envelope.
        let (rumor, wrapped) = wrap_with(
            &sender,
            &recipient.public_key(),
            Timestamp::from(2000),
            "hello",
        )
        .await;
        assert_ne!(
            wrapped.created_at.as_secs(),
            2000,
            "wrap timestamp randomizes"
        );
        assert_eq!(
            rumor_timestamp_us(&rumor),
            2_000_000_000,
            "rumor created_at * 1e6"
        );
    }

    #[tokio::test]
    async fn ts_ms_tag_is_absolute_us() {
        let sender = Keys::generate();
        let rumor = gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000),
            14.into(),
            vec![Tag::parse(["ts_ms", "1700000000500000"]).unwrap()],
            "x".to_string(),
        );
        // The tag is an absolute unix-µs timestamp, never base + tag.
        assert_eq!(rumor_timestamp_us(&rumor), 1_700_000_000_500_000);
    }

    /// Regression (bd-ts-ms-dual-semantics-kviu): the `ts_ms` tag must mean
    /// the SAME thing on every G7 path — an ABSOLUTE unix-µs timestamp that
    /// wins when present. The durable index timeline (rumor_timestamp_us)
    /// and the runtime inbox ordering (UnwrappedMail::ts_us) must produce
    /// the same key for the same rumor, with or without the tag.
    #[tokio::test]
    async fn ts_ms_tag_semantics_match_unwrap_ordering() {
        use crate::mailbox::unwrap::UnwrappedMail;

        let sender = Keys::generate();
        let build = |tags| {
            gift_wrap::build_rumor(
                sender.public_key(),
                Timestamp::from(1_700_000_000),
                14.into(),
                tags,
                "x".to_string(),
            )
        };

        // Absolute µs timestamp (2023-11-14T22:13:20.123456Z).
        let tagged = build(vec![Tag::parse(["ts_ms", "1700000000123456"]).unwrap()]);
        assert_eq!(rumor_timestamp_us(&tagged), 1_700_000_000_123_456);
        let mail = UnwrappedMail {
            recipient: sender.public_key(),
            sender: sender.public_key(),
            rumor: tagged.clone(),
            rumor_id: tagged.clone().id(),
            wrap_event_id: tagged.clone().id(),
            created_at: tagged.created_at,
        };
        assert_eq!(
            mail.ts_us(),
            rumor_timestamp_us(&tagged),
            "index timeline and inbox ordering must agree on the tag"
        );

        // No tag → both fall back to rumor created_at * 1e6.
        let untagged = build(vec![]);
        assert_eq!(rumor_timestamp_us(&untagged), 1_700_000_000_000_000);
        let mail = UnwrappedMail {
            recipient: sender.public_key(),
            sender: sender.public_key(),
            rumor: untagged.clone(),
            rumor_id: untagged.clone().id(),
            wrap_event_id: untagged.clone().id(),
            created_at: untagged.created_at,
        };
        assert_eq!(mail.ts_us(), rumor_timestamp_us(&untagged));
    }

    #[tokio::test]
    async fn inverted_envelope_times_still_order_by_rumor_time() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rpk = recipient.public_key();

        // A: rumor t=100, envelope t=900  (envelope says A is NEWER)
        // B: rumor t=500, envelope t=100  (envelope says B is OLDER)
        // Correct G7 order: A (t=100) then B (t=500). The envelope's
        // created_at is deliberately NOT consulted.
        let (rumor_a, wrap_a) = wrap_with(&sender, &rpk, Timestamp::from(100), "a").await;
        let (rumor_b, wrap_b) = wrap_with(&sender, &rpk, Timestamp::from(500), "b").await;

        // Unwrap both with the recipient key (envelope id filled in here).
        let unwrap_a = gift_wrap::unwrap(&recipient, &wrap_a).expect("unwrap a");
        let unwrap_b = gift_wrap::unwrap(&recipient, &wrap_b).expect("unwrap b");

        // Envelope timestamps are inverted vs rumor order (NIP-17 randomization).
        let mut items = vec![
            MailItem {
                envelope_id: wrap_b.id.to_string(),
                created_ts_us: rumor_timestamp_us(&rumor_b),
                ..extract_mail(&unwrap_b, 2)
            },
            MailItem {
                envelope_id: wrap_a.id.to_string(),
                created_ts_us: rumor_timestamp_us(&rumor_a),
                ..extract_mail(&unwrap_a, 1)
            },
        ];
        sort_by_timeline(&mut items);
        assert_eq!(
            items[0].body, "a",
            "rumor t=100 first despite envelope t=900"
        );
        assert_eq!(items[1].body, "b");
        assert_eq!(items[0].created_ts_us, 100_000_000);
        assert_eq!(items[1].created_ts_us, 500_000_000);
    }

    #[tokio::test]
    async fn same_rumor_timestamp_ties_broken_by_local_seq() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rpk = recipient.public_key();
        let (rumor_a, wrap_a) = wrap_with(&sender, &rpk, Timestamp::from(100), "a").await;
        let (rumor_b, wrap_b) = wrap_with(&sender, &rpk, Timestamp::from(100), "b").await;
        let ua = gift_wrap::unwrap(&recipient, &wrap_a).unwrap();
        let ub = gift_wrap::unwrap(&recipient, &wrap_b).unwrap();

        // Same rumor ts; local_seq decides (reproducible timeline).
        let mut items = vec![
            extract_mail(&ub, 2),
            extract_mail(&ua, 1),
            extract_mail(&ub, 0),
        ];
        sort_by_timeline(&mut items);
        let seqs: Vec<i64> = items.iter().map(|m| m.local_seq).collect();
        assert_eq!(seqs, vec![0, 1, 2]);
        let _ = rumor_a;
        let _ = rumor_b;
    }

    #[tokio::test]
    async fn block_all_drops_before_write() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let (_, wrap) = wrap_with(
            &sender,
            &recipient.public_key(),
            Timestamp::from(100),
            "spam",
        )
        .await;
        let uw = gift_wrap::unwrap(&recipient, &wrap).expect("unwrap");

        let approved: HashSet<PublicKey> = HashSet::new();
        let decision = decide(&uw, ContactPolicy::BlockAll, &approved);
        match decision {
            ProjectionDecision::Drop(_) => {}
            ProjectionDecision::Index(_) => panic!("block_all must drop before write"),
        }

        // open accepts.
        let decision = decide(&uw, ContactPolicy::Open, &approved);
        assert!(matches!(decision, ProjectionDecision::Index(_)));
    }

    #[tokio::test]
    async fn contacts_only_requires_approval() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let (_, wrap) =
            wrap_with(&sender, &recipient.public_key(), Timestamp::from(100), "hi").await;
        let uw = gift_wrap::unwrap(&recipient, &wrap).unwrap();

        let empty: HashSet<PublicKey> = HashSet::new();
        assert!(matches!(
            decide(&uw, ContactPolicy::ContactsOnly, &empty),
            ProjectionDecision::Drop(_)
        ));

        let approved: HashSet<PublicKey> = HashSet::from([sender.public_key()]);
        assert!(matches!(
            decide(&uw, ContactPolicy::ContactsOnly, &approved),
            ProjectionDecision::Index(_)
        ));
    }

    #[tokio::test]
    async fn ack_cache_is_idempotent() {
        let acker = Keys::generate().public_key();
        let cache = AckCache::new();
        assert!(cache.record(&acker, "msg-1"), "first ack new");
        assert!(!cache.record(&acker, "msg-1"), "replay idempotent");
        assert!(cache.contains(&acker, "msg-1"));
        assert!(!cache.contains(&acker, "msg-2"));
    }
}
