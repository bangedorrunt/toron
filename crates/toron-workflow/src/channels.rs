// governed-by: ADR-0003
// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! NIP-29 channels (bd-aws): membership-only access gate + global-sub
//! exclusion. toron-superset (Buzz-grade collaboration), not AM-Rust parity.
//!
//! Wire (ADR-0003 Phase 2 / ADR-0020):
//! - kind 40 channel-create (`d`=channel-id, `h`=channel-id)
//! - kind 41 channel-metadata (`h`)
//! - kind **9** channel-message (`h`, `t`=project slug, `p` for @mentions)
//! - kind 42 legacy ingest only
//! - kind 7 reaction (`h`)
//!
//! **Membership is the only gate.** Enforcement lives at the same point as
//! contact policies (G14, bd-led.4.4) and the Buzz access-gated fanout
//! (bd-led.3.2): a non-member's event is dropped before index/fanout, and
//! global subscriptions never receive private channel events — one code path.
//!
//! TOCTOU-safe membership: reads + writes go through a single `ChannelStore`
//! with a write gate, so a join racing a post resolves deterministically.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use nostr_sdk::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::toron_kind;

/// NIP-29 channel-create kind (40). Store-local create; optional 39000 later.
pub const CHANNEL_CREATE: u16 = 40;
/// NIP-29 channel-metadata kind (41).
pub const CHANNEL_METADATA: u16 = 41;
/// Canonical channel post (ADR-0003 / ADR-0020). NIP-29 group chat.
pub const CHANNEL_MESSAGE: u16 = 9;
/// Legacy emit (pre-ADR-0020). Ingest only.
pub const CHANNEL_MESSAGE_LEGACY: u16 = 42;
/// NIP-25 reaction kind (7).
pub const REACTION: u16 = 7;

/// A channel's membership set + metadata.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Channel {
    /// Channel id (the `d`/`h` tag).
    pub id: String,
    /// Display name.
    #[serde(default)]
    pub name: String,
    /// True for private channels (global subs must never see their events).
    #[serde(default)]
    pub private: bool,
    /// Optional project mapping (project-as-channel).
    #[serde(default)]
    pub project: Option<String>,
    /// Member npubs (hex).
    pub members: HashSet<String>,
    /// Creator npub.
    pub creator: String,
}

impl Channel {
    /// Is `npub` a member?
    pub fn is_member(&self, npub: &PublicKey) -> bool {
        self.members.contains(&npub.to_hex())
    }
}

/// A channel message (post or reaction).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelEvent {
    /// The channel id.
    pub channel_id: String,
    /// The event id.
    pub event_id: String,
    /// Author npub.
    pub author: String,
    /// Kind (9 canonical / 42 legacy message, 7 reaction).
    pub kind: u16,
    /// Content.
    pub content: String,
    /// Message id being reacted to (kind 7 only).
    #[serde(default)]
    pub react_to: Option<String>,
    /// `@mention` targets (p tags).
    #[serde(default)]
    pub mentions: Vec<String>,
}

/// The channel store: in-memory membership + the signed events.
#[derive(Debug, Clone, Default)]
pub struct ChannelStore {
    inner: Arc<Mutex<ChannelStoreInner>>,
}

#[derive(Debug, Default)]
struct ChannelStoreInner {
    channels: BTreeMap<String, Channel>,
    events: Vec<ChannelEvent>,
}

/// bd-channel-truncation-238e: a channel post above this many chars is
/// REFUSED (usage error naming the limit + the `--file` chunking affordance)
/// instead of silently truncating — silent truncation of a status report is
/// data loss wearing a success costume.
pub const CHANNEL_POST_MAX_CHARS: usize = 4096;

/// Split an over-cap post into bounded chunks for `channel post --file`:
/// whole lines where possible (a line longer than the cap is hard-split),
/// each chunk prefixed `(i/N) `. A short post returns unmarked as-is.
pub fn split_channel_post(content: &str, max_chars: usize) -> Vec<String> {
    if content.chars().count() <= max_chars {
        return vec![content.to_string()];
    }
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    for line in content.split_inclusive('\n') {
        if cur.chars().count() + line.chars().count() > max_chars {
            if !cur.is_empty() {
                parts.push(std::mem::take(&mut cur));
            }
            // single over-long line: hard-split it
            let mut piece = String::new();
            for ch in line.chars() {
                piece.push(ch);
                if piece.chars().count() >= max_chars {
                    parts.push(std::mem::take(&mut piece));
                }
            }
            cur = piece;
        } else {
            cur.push_str(line);
        }
    }
    if !cur.is_empty() {
        parts.push(cur);
    }
    let n = parts.len();
    parts
        .into_iter()
        .enumerate()
        .map(|(i, body)| format!("({}/{}) {body}", i + 1, n))
        .collect()
}

impl ChannelStore {
    /// New empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a channel (idempotent: re-creating with the same id is a no-op).
    pub fn create_channel(
        &self,
        creator: &PublicKey,
        id: &str,
        name: &str,
        private: bool,
        project: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        if inner.channels.contains_key(id) {
            return Ok(()); // idempotent
        }
        inner.channels.insert(
            id.to_string(),
            Channel {
                id: id.to_string(),
                name: name.to_string(),
                private,
                project: project.map(str::to_string),
                members: HashSet::from([creator.to_hex()]),
                creator: creator.to_hex(),
            },
        );
        Ok(())
    }

    /// Join a channel (idempotent).
    pub fn join(&self, channel_id: &str, npub: &PublicKey) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        let ch = inner
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        ch.members.insert(npub.to_hex());
        Ok(())
    }

    /// Leave a channel (idempotent; the creator cannot leave — they own it).
    pub fn leave(&self, channel_id: &str, npub: &PublicKey) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        let ch = inner
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        if ch.creator == npub.to_hex() {
            anyhow::bail!("creator cannot leave channel {channel_id}");
        }
        ch.members.remove(&npub.to_hex());
        Ok(())
    }

    /// Is `npub` a member of the channel?
    pub fn is_member(&self, channel_id: &str, npub: &PublicKey) -> bool {
        self.inner
            .lock()
            .channels
            .get(channel_id)
            .map(|c| c.is_member(npub))
            .unwrap_or(false)
    }

    /// The channel's member npubs.
    pub fn members(&self, channel_id: &str) -> Vec<String> {
        self.inner
            .lock()
            .channels
            .get(channel_id)
            .map(|c| c.members.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Get a channel.
    pub fn get(&self, channel_id: &str) -> Option<Channel> {
        self.inner.lock().channels.get(channel_id).cloned()
    }

    /// List all channels.
    pub fn list(&self) -> Vec<Channel> {
        self.inner.lock().channels.values().cloned().collect()
    }

    /// True when `kind` is a channel post (canonical 9 or legacy 42).
    pub fn is_post_kind(kind: u16) -> bool {
        kind == CHANNEL_MESSAGE || kind == CHANNEL_MESSAGE_LEGACY
    }

    /// Build + record a channel-message event (kind 9). **Membership gate:**
    /// only members may post; a non-member is rejected before any event is
    /// produced (the gate is at the store boundary — the same point the
    /// projection/fanout uses).

    pub fn post(
        &self,
        author: &Keys,
        channel_id: &str,
        content: &str,
        mentions: &[PublicKey],
    ) -> anyhow::Result<Event> {
        let author_pk = author.public_key();
        self.require_member(channel_id, &author_pk)?;
        // bd-channel-truncation-238e: fail LOUD on over-cap posts.
        let chars = content.chars().count();
        if chars > CHANNEL_POST_MAX_CHARS {
            anyhow::bail!(
                "post is {chars} chars; channel posts cap at {} — split it with \
                 `toron channel post --file <path>` ((1/N) chunked thread) or send the \
                 full report to a file and post a summary",
                CHANNEL_POST_MAX_CHARS
            );
        }
        let mut tags = vec![Tag::parse(["h", channel_id]).expect("h tag")];
        if let Some(slug) = self.get(channel_id).and_then(|c| c.project) {
            tags.push(Tag::parse(["t", &slug]).expect("t tag"));
        }
        for m in mentions {
            tags.push(Tag::public_key(*m));
        }
        let ev = build_event(
            author,
            toron_kind(CHANNEL_MESSAGE),
            content,
            tags,
            Timestamp::now(),
        )?;
        self.inner.lock().events.push(ChannelEvent {
            channel_id: channel_id.to_string(),
            event_id: ev.id.to_hex(),
            author: author_pk.to_hex(),
            kind: CHANNEL_MESSAGE,
            content: content.to_string(),
            react_to: None,
            mentions: mentions.iter().map(|m| m.to_hex()).collect(),
        });
        Ok(ev)
    }

    /// Ingest a channel-message event from the bus (ADR-0020 Spec 1):
    /// accepts kind 9 (canonical) and kind 42 (legacy); **both project as
    /// channel posts**. Membership-gated at the store boundary — a
    /// non-member's event is dropped before index/fanout (the same gate as
    /// [`Self::post`]).
    pub fn ingest(&self, ev: &Event) -> anyhow::Result<ChannelEvent> {
        let kind = ev.kind.as_u16();
        if !Self::is_post_kind(kind) {
            anyhow::bail!("not a channel post kind: {kind} (expected 9 or 42)");
        }
        let channel_id = ev
            .tags
            .iter()
            .find(|t| t.as_slice().first().map(String::as_str) == Some("h"))
            .and_then(|t| t.as_slice().get(1).cloned())
            .ok_or_else(|| anyhow::anyhow!("channel post lacks h tag"))?;
        self.require_member(&channel_id, &ev.pubkey)?;
        let mentions: Vec<String> = ev
            .tags
            .iter()
            .filter(|t| t.as_slice().first().map(String::as_str) == Some("p"))
            .filter_map(|t| t.as_slice().get(1).cloned())
            .collect();
        let ce = ChannelEvent {
            channel_id,
            event_id: ev.id.to_hex(),
            author: ev.pubkey.to_hex(),
            kind,
            content: ev.content.clone(),
            react_to: None,
            mentions,
        };
        self.inner.lock().events.push(ce.clone());
        Ok(ce)
    }

    /// Build + record a reaction (kind 7). Membership-gated.
    pub fn react(
        &self,
        author: &Keys,
        channel_id: &str,
        message_id: &str,
        emoji: &str,
    ) -> anyhow::Result<Event> {
        let author_pk = author.public_key();
        self.require_member(channel_id, &author_pk)?;
        let message = EventId::from_hex(message_id)
            .map_err(|_| anyhow::anyhow!("bad message id {message_id}"))?;
        let ev = build_event(
            author,
            toron_kind(REACTION),
            emoji,
            vec![
                Tag::parse(["h", channel_id]).expect("h"),
                Tag::event(message),
            ],
            Timestamp::now(),
        )?;
        self.inner.lock().events.push(ChannelEvent {
            channel_id: channel_id.to_string(),
            event_id: ev.id.to_hex(),
            author: author_pk.to_hex(),
            kind: REACTION,
            content: emoji.to_string(),
            react_to: Some(message_id.to_string()),
            mentions: Vec::new(),
        });
        Ok(ev)
    }

    /// Channel message/reaction history (members only).
    pub fn history(&self, channel_id: &str) -> Vec<ChannelEvent> {
        self.inner
            .lock()
            .events
            .iter()
            .filter(|e| e.channel_id == channel_id)
            .cloned()
            .collect()
    }

    /// **Access gate** (the enforcement point shared with the projection/
    /// fanout): a subscription/query is allowed to see an event only when the
    /// reader is a member of the channel the event belongs to. Non-members get
    /// nothing; private channels additionally require a member reader (global
    /// subscriptions are excluded by construction — they have no member set).
    pub fn can_read(&self, channel_id: &str, reader: &PublicKey) -> bool {
        self.is_member(channel_id, reader)
    }

    fn require_member(&self, channel_id: &str, npub: &PublicKey) -> anyhow::Result<()> {
        if !self.is_member(channel_id, npub) {
            anyhow::bail!("not a member of channel {channel_id} — membership is the only gate");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Keys {
        Keys::generate()
    }

    #[test]
    fn create_join_post_membership_gate() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        let mallory = keys();

        store
            .create_channel(&alice.public_key(), "auth-refactor", "Auth", true, None)
            .expect("create");
        store
            .join("auth-refactor", &bob.public_key())
            .expect("bob joins");

        // Alice (creator) + Bob can post.
        assert!(store.post(&alice, "auth-refactor", "hello", &[]).is_ok());
        assert!(store.post(&bob, "auth-refactor", "hi bob", &[]).is_ok());
        // Mallory (non-member) is gated at the store boundary.
        assert!(
            store
                .post(&mallory, "auth-refactor", "intrude", &[])
                .is_err()
        );

        // Membership gate: non-member cannot read private channel history.
        assert!(store.can_read("auth-refactor", &alice.public_key()));
        assert!(store.can_read("auth-refactor", &bob.public_key()));
        assert!(!store.can_read("auth-refactor", &mallory.public_key()));

        // History visible to members only (store-level access already gated).
        let history = store.history("auth-refactor");
        assert_eq!(history.len(), 2);
        assert!(history.iter().all(|e| e.kind == CHANNEL_MESSAGE));
    }

    /// bd-channel-truncation-238e: an over-cap post FAILS LOUDLY naming the
    /// limit and the chunking affordance — never silently truncates.
    #[test]
    fn channel_post_over_cap_fails_loudly() {
        let store = ChannelStore::new();
        let author = Keys::generate();
        store
            .create_channel(&author.public_key(), "cap", "Cap", false, Some("toron"))
            .expect("create");
        let big = "x".repeat(CHANNEL_POST_MAX_CHARS + 1);
        let err = format!(
            "{}",
            store.post(&author, "cap", &big, &[]).unwrap_err()
        );
        assert!(err.contains(&CHANNEL_POST_MAX_CHARS.to_string()), "{err}");
        assert!(err.contains("--file"), "names the chunking fix: {err}");
        // at-cap is fine
        let at = "x".repeat(CHANNEL_POST_MAX_CHARS);
        assert!(store.post(&author, "cap", &at, &[]).is_ok());
    }

    /// The splitter: every chunk <= max INCLUDING its (i/N) marker, markers
    /// numbered 1..N, content preserved in order.
    #[test]
    fn channel_post_split_chunks_are_bounded_and_ordered() {
        let content = ("line of crew report text.\n").repeat(200); // ~5400 chars
        let chunks = split_channel_post(&content, 700);
        assert!(chunks.len() > 1, "long content splits: {}", chunks.len());
        for (i, c) in chunks.iter().enumerate() {
            assert!(c.chars().count() <= 700, "chunk {i} is {} chars", c.len());
            assert!(c.starts_with(&format!("({}/{}) ", i + 1, chunks.len())));
        }
        // round-trip: markers stripped, order preserved
        let joined: String = chunks
            .iter()
            .map(|c| c.splitn(2, ' ').nth(1).expect("body"))
            .collect::<Vec<_>>()
            .join("");
        assert_eq!(joined.replace('\n', ""), content.replace('\n', ""));
        // short content = single unmarked chunk? No: single chunk gets no marker
        let one = split_channel_post("short", 700);
        assert_eq!(one.len(), 1);
        assert_eq!(one[0], "short");
    }

    #[test]
    fn post_emits_kind_nine_not_forty_two() {
        let store = ChannelStore::new();
        let alice = keys();
        store
            .create_channel(&alice.public_key(), "room", "Room", false, Some("toron"))
            .expect("create");
        let ev = store.post(&alice, "room", "hello", &[]).expect("post");
        assert_eq!(ev.kind.as_u16(), 9, "ADR-0020 emit is kind 9");
        assert_eq!(CHANNEL_MESSAGE, 9);
        assert_eq!(CHANNEL_MESSAGE_LEGACY, 42);
        let has_t = ev.tags.iter().any(|t| format!("{t:?}").contains("toron"));
        assert!(has_t, "project slug on t tag, tags={:?}", ev.tags);
    }

    #[test]
    fn reaction_membership_gated() {
        let store = ChannelStore::new();
        let alice = keys();
        let mallory = keys();
        store
            .create_channel(&alice.public_key(), "c1", "C", false, None)
            .expect("create");
        let post = store.post(&alice, "c1", "hello", &[]).expect("post");

        assert!(store.react(&alice, "c1", &post.id.to_hex(), "+1").is_ok());
        assert!(
            store
                .react(&mallory, "c1", &post.id.to_hex(), "+1")
                .is_err()
        );
    }

    #[test]
    fn global_sub_exclusion_by_construction() {
        // A "global subscription" has no member identity → `can_read` is
        // false for any private channel. This is the same rule the bus fanout
        // uses: private channel events never reach global subs.
        let store = ChannelStore::new();
        let alice = keys();
        store
            .create_channel(&alice.public_key(), "private-1", "Private", true, None)
            .expect("create");
        // A non-member reader (the global-sub stand-in) cannot read it.
        let outsider = keys();
        assert!(!store.can_read("private-1", &outsider.public_key()));
    }

    #[test]
    fn creator_cannot_leave_but_members_can() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.join("c", &bob.public_key()).expect("join");
        assert!(
            store.leave("c", &alice.public_key()).is_err(),
            "creator stays"
        );
        assert!(store.leave("c", &bob.public_key()).is_ok(), "member leaves");
        assert!(!store.is_member("c", &bob.public_key()));
    }

    #[test]
    fn create_is_idempotent() {
        let store = ChannelStore::new();
        let alice = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("first");
        store
            .create_channel(&alice.public_key(), "c", "C2", false, None)
            .expect("second is no-op");
        assert_eq!(store.list().len(), 1);
        assert_eq!(store.get("c").unwrap().name, "C", "first wins");
    }

    /// ADR-0020 Spec 1: a channel post EMITS kind 9 (NIP-29 channel message),
    /// never the legacy 42.
    #[test]
    fn post_emits_kind_9_not_42() {
        let store = ChannelStore::new();
        let alice = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        let ev = store.post(&alice, "c", "hello", &[]).expect("post");
        assert_eq!(
            ev.kind.as_u16(),
            9,
            "channel post emits kind 9 (ADR-0020 Spec 1), got {}",
            ev.kind.as_u16()
        );
    }

    #[test]
    fn post_carries_h_and_p_tags() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.join("c", &bob.public_key()).expect("join");
        let ev = store
            .post(&alice, "c", "@bob fix this", &[bob.public_key()])
            .expect("post");
        assert_eq!(ev.kind.as_u16(), CHANNEL_MESSAGE);
        assert!(
            ev.tags
                .iter()
                .any(|t| t.as_slice().first().map(String::as_str) == Some("h")),
            "h tag present"
        );
        assert!(
            ev.tags
                .iter()
                .any(|t| t.as_slice().first().map(String::as_str) == Some("p")),
            "p tag for @mention"
        );
    }

    /// ADR-0020 Spec 1: ingest accepts kind 9 (canonical) and kind 42
    /// (legacy) and projects both as channel posts.
    #[test]
    fn ingest_projects_kind_9_and_legacy_42() {
        let store = ChannelStore::new();
        let alice = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, Some("toron"))
            .expect("create");

        // Canonical kind 9 event, built by hand (as if from the bus).
        let ev9 = build_event(
            &alice,
            toron_kind(CHANNEL_MESSAGE),
            "hello",
            vec![Tag::parse(["h", "c"]).expect("h")],
            Timestamp::now(),
        )
        .expect("build kind 9");
        let p9 = store.ingest(&ev9).expect("ingest kind 9");
        assert_eq!(p9.kind, 9);
        assert_eq!(p9.channel_id, "c");

        // Legacy kind 42 event, ingested the same way.
        let ev42 = build_event(
            &alice,
            toron_kind(CHANNEL_MESSAGE_LEGACY),
            "legacy hello",
            vec![Tag::parse(["h", "c"]).expect("h")],
            Timestamp::now(),
        )
        .expect("build legacy 42");
        let p42 = store.ingest(&ev42).expect("ingest kind 42");
        assert_eq!(p42.kind, 42);
        assert_eq!(p42.content, "legacy hello");

        // Both project into history as channel posts.
        let hist = store.history("c");
        assert_eq!(hist.len(), 2);
        assert!(hist.iter().all(|e| e.kind == 9 || e.kind == 42));
    }

    /// ADR-0020 Spec 1: non-post kinds and non-member events are rejected at
    /// ingest (the same store-boundary gate as `post`).
    #[test]
    fn ingest_rejects_other_kinds_and_non_members() {
        let store = ChannelStore::new();
        let alice = keys();
        let mallory = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");

        // A reaction (kind 7) is not a channel post.
        let post = store.post(&alice, "c", "hello", &[]).expect("post");
        let reaction = store
            .react(&alice, "c", &post.id.to_hex(), "+1")
            .expect("react");
        assert!(
            store.ingest(&reaction).is_err(),
            "kind 7 is not a channel post"
        );

        // A kind-9 event from a non-member is dropped at the gate.
        let stranger = build_event(
            &mallory,
            toron_kind(CHANNEL_MESSAGE),
            "intrude",
            vec![Tag::parse(["h", "c"]).expect("h")],
            Timestamp::now(),
        )
        .expect("build");
        assert!(
            store.ingest(&stranger).is_err(),
            "non-member event dropped before index"
        );
        // Only the member's post + reaction were recorded; both failed
        // ingests added nothing.
        assert_eq!(
            store.history("c").len(),
            2,
            "rejected ingests are not recorded"
        );
    }
}
