// governed-by: ADR-0025
// governed-by: ADR-0003
// governed-by: ADR-0020
// governed-by: ADR-0023
#![forbid(unsafe_code)]

//! NIP-29 channels (bd-aws): membership-only access gate + global-sub
//! exclusion. toron-superset (Buzz-grade collaboration), not AM-Rust parity.
//!
//! Wire (ADR-0003 Phase 2 / ADR-0020 / ADR-0023):
//! - kind **39000** group metadata (`d`=`h`=channel-id; create/close)
//! - kind **39002** group members (`d`=`h`; `p`=each member)
//! - kind **9** channel-message (`h`, `t`=project slug, `p` for @mentions)
//! - kind 40/41/42 legacy ingest only
//! - kind 7 reaction (`h`, `p`, `k`)
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

use nostr::prelude::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use toron_core::bus::convergence::{
    AuthorityOp, Convergence, ConvergenceEffect, ConvergenceMsg, tick as convergence_tick,
};
use toron_core::nostr::event::build_event;
use toron_core::nostr::kinds::toron_kind;

/// NIP-29 group metadata (ADR-0023). Addressable create/close.
pub const CHANNEL_CREATE: u16 = 39000;
/// NIP-29 group members snapshot (ADR-0023).
pub const CHANNEL_MEMBERS: u16 = 39002;
/// Pre-ADR-0023 NIP-28 create. Ingest only.
pub const CHANNEL_CREATE_LEGACY: u16 = 40;
/// Pre-ADR-0023 NIP-28 join/leave/close. Ingest only.
pub const CHANNEL_METADATA_LEGACY: u16 = 41;
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
    /// True once the creator retires the channel (bd-channel-close-purge-bsqb):
    #[serde(default)]
    pub closed: bool,
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
    /// In-flight / last authority op per channel (GS-062 Kick/Disable convergence).
    authority: BTreeMap<String, Convergence>,
    /// Monotonic generation for Propose (re-add fence key).
    authority_generation: u64,
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
                closed: false,
                members: HashSet::from([creator.to_hex()]),
                creator: creator.to_hex(),
            },
        );
        drop(inner);
        Ok(())
    }

    /// Join a channel (idempotent).
    ///
    /// GS-062 / Buzz #7298 (ask-user A): ReAdd fences when Kick/Disable is held.
    /// Production leave/close hold authority after FireLive; settle_authority ends it.
    pub fn join(&self, channel_id: &str, npub: &PublicKey) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        let ch = inner
            .channels
            .get(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        // bd-channel-close-purge-bsqb: a retired channel accepts nobody new.
        if ch.closed {
            anyhow::bail!("channel {channel_id} is closed — join is refused");
        }
        if let Some(held) = inner.authority.get(channel_id) {
            let held_gen = match held {
                Convergence::Pending { generation, .. }
                | Convergence::Committed { generation, .. } => *generation,
                Convergence::Fenced {
                    held_generation, ..
                } => *held_generation,
                Convergence::Rejected { .. } => 0,
            };
            if !matches!(held, Convergence::Rejected { .. }) {
                // Always ≠ held_gen so other channels' global counter bumps
                // cannot accidentally equal `held` and skip FenceReAdd.
                let attempted = held_gen.wrapping_add(1);
                let (next, fx) = convergence_tick(
                    Some(held),
                    &ConvergenceMsg::ReAdd {
                        generation: attempted,
                    },
                );
                if fx
                    .iter()
                    .any(|e| matches!(e, ConvergenceEffect::FenceReAdd { .. }))
                {
                    if let Some(state) = next {
                        inner.authority.insert(channel_id.to_string(), state);
                    }
                    anyhow::bail!(
                        "channel {channel_id} re-add fenced under held authority generation"
                    );
                }
            }
        }
        let ch = inner
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        ch.members.insert(npub.to_hex());
        drop(inner);
        Ok(())
    }

    /// Leave a channel (idempotent). The creator cannot leave an OPEN
    /// channel — they own it — but MAY leave once it is closed
    /// (bd-channel-close-purge-bsqb leave-rule fix).
    ///
    /// GS-062: membership mutation is the FireLive host action after
    /// Propose→Commit for [`AuthorityOp::Kick`] (Buzz #7298 convergence).
    pub fn leave(&self, channel_id: &str, npub: &PublicKey) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        let ch = inner
            .channels
            .get(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        if ch.creator == npub.to_hex() && !ch.closed {
            anyhow::bail!(
                "creator cannot leave channel {channel_id} while it is open                  (close it first: toron channel close {channel_id})"
            );
        }
        let target = npub.to_hex();
        let generation = inner.authority_generation.saturating_add(1);
        inner.authority_generation = generation;
        let op = AuthorityOp::Kick {
            target: target.clone(),
        };
        let (pending, propose_fx) = convergence_tick(
            None,
            &ConvergenceMsg::Propose {
                op: op.clone(),
                generation,
            },
        );
        anyhow::ensure!(
            propose_fx.is_empty(),
            "kick propose must not fanout before commit"
        );
        let (committed, commit_fx) = convergence_tick(pending.as_ref(), &ConvergenceMsg::Commit);
        let fired = commit_fx.iter().any(|e| {
            matches!(
                e,
                ConvergenceEffect::FireLive {
                    op: AuthorityOp::Kick { target: t },
                    generation: g,
                } if t == &target && *g == generation
            )
        });
        anyhow::ensure!(fired, "kick FireLive missing after Commit");
        if let Some(state) = committed {
            inner.authority.insert(channel_id.to_string(), state);
        }
        // FireLive host action: mutate membership (single SoT).
        let ch = inner
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        ch.members.remove(&target);
        // Ask-user A supersession (GS-062): HOLD Committed across the
        // post-FireLive critical section so a production join hits FenceReAdd.
        // (Authority already inserted above.) End with [`Self::settle_authority`].
        drop(inner);
        Ok(())
    }

    /// End the post-FireLive authority critical section (GS-062 ask-user A).
    /// After settle, join is no longer ReAdd-fenced by this Kick/Disable hold.
    /// Idempotent.
    pub fn settle_authority(&self, channel_id: &str) {
        self.inner.lock().authority.remove(channel_id);
    }

    /// Test helper: insert a held convergence state. Production leave/close
    /// already hold after FireLive; this remains for unit tests that skip leave.
    pub fn hold_authority_for_test(&self, channel_id: &str, state: Convergence) {
        self.inner
            .lock()
            .authority
            .insert(channel_id.to_string(), state);
    }

    /// Close (retire) a channel — bd-channel-close-purge-bsqb. Creator-only,
    /// idempotent. A closed channel refuses posts/joins/reactions; its
    /// history survives until `toron channel purge` removes the bus events.
    ///
    /// GS-062: `closed = true` is the FireLive host action after
    /// Propose→Commit for [`AuthorityOp::Disable`].
    pub fn close(&self, channel_id: &str, by: &PublicKey) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        let ch = inner
            .channels
            .get(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        if ch.creator != by.to_hex() {
            anyhow::bail!("only the creator can close channel {channel_id}");
        }
        let target = channel_id.to_string();
        let generation = inner.authority_generation.saturating_add(1);
        inner.authority_generation = generation;
        let op = AuthorityOp::Disable {
            target: target.clone(),
        };
        let (pending, propose_fx) = convergence_tick(
            None,
            &ConvergenceMsg::Propose {
                op: op.clone(),
                generation,
            },
        );
        anyhow::ensure!(propose_fx.is_empty(), "disable propose must not fanout");
        let (committed, commit_fx) = convergence_tick(pending.as_ref(), &ConvergenceMsg::Commit);
        let fired = commit_fx.iter().any(|e| {
            matches!(
                e,
                ConvergenceEffect::FireLive {
                    op: AuthorityOp::Disable { target: t },
                    generation: g,
                } if t == &target && *g == generation
            )
        });
        anyhow::ensure!(fired, "disable FireLive missing after Commit");
        if let Some(state) = committed {
            inner.authority.insert(channel_id.to_string(), state);
        }
        let ch = inner
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        ch.closed = true;
        // Ask-user A supersession: HOLD Disable across critical section.
        drop(inner);
        Ok(())
    }

    /// Replace the member set from a kind-39002 snapshot (ADR-0023).
    pub fn replace_members(
        &self,
        channel_id: &str,
        members: impl IntoIterator<Item = String>,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.lock();
        let ch = inner
            .channels
            .get_mut(channel_id)
            .ok_or_else(|| anyhow::anyhow!("channel not found: {channel_id}"))?;
        ch.members = members.into_iter().collect();
        drop(inner);
        Ok(())
    }

    /// Rebuild a store from wire lifecycle events (bd-e03q, ADR-0035 Spec 4):
    /// the bus 39000/39002 sequence (legacy 40/41) is the durable truth and
    /// this store is its single-writer in-memory projection. Fresh processes
    /// replay in wire order; later events win (a close retire beats an
    /// earlier join, a 39002 snapshot replaces the set). Structural breaks
    /// (missing channel id) fail the rebuild — a half-room is worse than none.
    pub fn rebuild(events: impl IntoIterator<Item = Event>) -> anyhow::Result<Self> {
        let store = Self::new();
        for ev in events {
            store.apply_lifecycle(&ev)?;
        }
        Ok(store)
    }

    /// Apply one lifecycle event to the projection. Returns whether the
    /// store changed: unknown kinds and inapplicable transitions (a join for
    /// a room that never existed, a close by a non-creator) are valid stream
    /// dynamics and report `false` — only a structurally unreadable event
    /// (no channel id) is a rebuild-stopping error.
    pub fn apply_lifecycle(&self, ev: &Event) -> anyhow::Result<bool> {
        match ev.kind.as_u16() {
            CHANNEL_CREATE | CHANNEL_CREATE_LEGACY => {
                let id = Self::lifecycle_channel_id(ev)
                    .ok_or_else(|| anyhow::anyhow!("channel create lacks d/h tag"))?;
                let name = ev
                    .tags
                    .iter()
                    .find(|t| t.as_slice().first().map(String::as_str) == Some("name"))
                    .and_then(|t| t.as_slice().get(1).cloned())
                    .unwrap_or_default();
                let private = ev.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("private")
                        && t.as_slice().get(1).map(String::as_str) == Some("true")
                });
                let project = ev
                    .tags
                    .iter()
                    .find(|t| t.as_slice().first().map(String::as_str) == Some("t"))
                    .and_then(|t| t.as_slice().get(1).cloned());
                self.create_channel(&ev.pubkey, &id, &name, private, project.as_deref())?;
                let closed = ev.tags.iter().any(|t| {
                    t.as_slice().first().map(String::as_str) == Some("closed")
                        && t.as_slice().get(1).map(String::as_str) != Some("false")
                });
                if closed {
                    // A close inside the create event retires immediately;
                    // a non-creator close tag is an inapplicable no-op.
                    if self.close(&id, &ev.pubkey).is_err() {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            CHANNEL_MEMBERS => {
                let id = Self::lifecycle_channel_id(ev)
                    .ok_or_else(|| anyhow::anyhow!("channel members snapshot lacks d/h tag"))?;
                // Content JSON is the snapshot source of truth: rust-nostr
                // strips a p-tag that equals the signer, which would drop the
                // joiner from a 39002 they just signed.
                let members: Vec<String> = serde_json::from_str(&ev.content).unwrap_or_else(|_| {
                    ev.tags
                        .iter()
                        .filter(|t| t.as_slice().first().map(String::as_str) == Some("p"))
                        .filter_map(|t| t.as_slice().get(1).cloned())
                        .collect()
                });
                if self.replace_members(&id, members).is_err() {
                    return Ok(false);
                }
                Ok(true)
            }
            CHANNEL_METADATA_LEGACY => {
                let Some(id) = ev
                    .tags
                    .iter()
                    .find(|t| t.kind() == "h")
                    .and_then(|t| t.content())
                else {
                    anyhow::bail!("legacy channel metadata lacks h tag");
                };
                // GS-062: leave/close HOLD after FireLive for in-process CS
                // (captain A). apply_lifecycle is rebuild/replay — settle so
                // replayed Kick does not permanently FenceReAdd joins.
                // Live leave()/close() still hold; CLI/host settle after publish CS.
                let applied = match ev.content.as_str() {
                    "join" => self.join(id, &ev.pubkey).is_ok(),
                    "leave" => {
                        let ok = self.leave(id, &ev.pubkey).is_ok();
                        if ok {
                            self.settle_authority(id);
                        }
                        ok
                    }
                    "close" => {
                        let ok = self.close(id, &ev.pubkey).is_ok();
                        if ok {
                            self.settle_authority(id);
                        }
                        ok
                    }
                    _ => false,
                };
                Ok(applied)
            }
            _ => Ok(false),
        }
    }

    /// The channel id of a lifecycle event: `d` first, `h` as fallback
    /// (both spellings ride the wire across the ADR-0023 cutover).
    fn lifecycle_channel_id(ev: &Event) -> Option<String> {
        ev.tags
            .iter()
            .find(|t| t.kind() == "d")
            .and_then(|t| t.content())
            .or_else(|| {
                ev.tags
                    .iter()
                    .find(|t| t.kind() == "h")
                    .and_then(|t| t.content())
            })
            .map(str::to_string)
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
    pub const fn is_post_kind(kind: u16) -> bool {
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
        // Spec 2: kind-9 for >20 lines without a path is rejected (must be file-first)
        if content.lines().count() > 20
            && !content.contains("path:")
            && !content.contains("sha256:")
            && !content.contains("Path:")
            && !content.contains("sha256")
        {
            anyhow::bail!(
                "kind-9 over 20 lines without path rejected — write findings to a file and post summary + path + sha256:12"
            );
        }
        // bd-channel-truncation-238e: fail LOUD on over-cap posts.
        let chars = content.chars().count();
        if chars > CHANNEL_POST_MAX_CHARS {
            anyhow::bail!(
                "post is {chars} chars; channel posts cap at {CHANNEL_POST_MAX_CHARS} — split it with \
                 `toron channel post --file <path>` ((1/N) chunked thread) or send the \
                 full report to a file and post a summary"
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
            mentions: mentions.iter().map(nostr::key::PublicKey::to_hex).collect(),
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

    /// Hydrate a channel message from the bus for cross-process react
    /// visibility (bd-13lb). Unlike `ingest`, this does **not** enforce
    /// membership of the target author — the target was already gated at post
    /// time. It validates kind (9/42) and `h` tag, deduplicates by `event_id`,
    /// and makes `react` see what `channel read` sees (bus projection)
    /// synchronously, without a sleep. The reactor's own membership is still
    /// enforced by `react` itself.
    pub fn hydrate_message(&self, ev: &Event) -> anyhow::Result<()> {
        let kind = ev.kind.as_u16();
        if !Self::is_post_kind(kind) {
            anyhow::bail!("not a channel post kind: {kind} (expected 9 or 42)");
        }
        let channel_id = ev
            .tags
            .iter()
            .find(|t| t.kind() == "h")
            .and_then(|t| t.content())
            .ok_or_else(|| anyhow::anyhow!("channel post lacks h tag"))?
            .to_string();
        let id_hex = ev.id.to_hex();
        {
            let inner = self.inner.lock();
            if inner.events.iter().any(|e| e.event_id == id_hex) {
                return Ok(());
            }
        }
        let mentions: Vec<String> = ev
            .tags
            .iter()
            .filter(|t| t.kind() == "p")
            .filter_map(|t| t.content())
            .map(str::to_string)
            .collect();
        let ce = ChannelEvent {
            channel_id,
            event_id: id_hex,
            author: ev.pubkey.to_hex(),
            kind,
            content: ev.content.clone(),
            react_to: None,
            mentions,
        };
        self.inner.lock().events.push(ce);
        Ok(())
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
        let target = self
            .inner
            .lock()
            .events
            .iter()
            .find(|e| e.event_id == message_id)
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!("unknown message {message_id} in channel {channel_id}")
            })?;
        let target_pk = PublicKey::from_hex(&target.author)
            .map_err(|e| anyhow::anyhow!("bad target author {}: {e}", target.author))?;
        let tags = vec![
            Tag::parse(["h", channel_id]).expect("h"),
            Tag::event(message),
            Tag::public_key(target_pk),
            Tag::parse(["k", &target.kind.to_string()]).expect("k"),
        ];
        let ev = build_event(author, toron_kind(REACTION), emoji, tags, Timestamp::now())?;
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
        let ch = self.inner.lock().channels.get(channel_id).cloned();
        let Some(ch) = ch else {
            anyhow::bail!("channel not found: {channel_id}");
        };
        // bd-channel-close-purge-bsqb: a closed (retired) channel refuses
        // writes outright — member or not.
        if ch.closed {
            anyhow::bail!(
                "channel {channel_id} is closed — posts and reactions are refused \
                 (removal: toron channel purge {channel_id})"
            );
        }
        if ch.is_member(npub) {
            return Ok(());
        }
        // bd-membership-gate-remedy-uti4: the gate names its remedy — a
        // bounce without the fix costs a help-page round-trip. The project
        // is named when the channel knows it, else a placeholder.
        let project = ch
            .project
            .as_deref()
            .map(|p| format!("--project {p}"))
            .unwrap_or_else(|| "--project <p>".to_string());
        anyhow::bail!(
            "not a member of channel {channel_id} — membership is the only gate; \
             run: toron channel join {channel_id} {project} --as <you>"
        );
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

    /// bd-kbl1c / bd-join-before-post-axfm: spawn-time channel membership
    /// bootstrap — join-before-post. Membership is the only gate; spawn must
    /// establish it before first doorbell. The store-level gate is the same
    /// as `create_join_post_membership_gate` but this name is the verify
    /// entrypoint (`cargo test --offline join_before_post`).
    #[test]
    fn join_before_post() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        let mallory = keys();
        store
            .create_channel(
                &alice.public_key(),
                "steer-axfm",
                "Steer",
                false,
                Some("toron"),
            )
            .expect("create");
        // Bob joins before post — the bootstrap that must happen at spawn.
        store
            .join("steer-axfm", &bob.public_key())
            .expect("bob joins");
        assert!(
            store
                .post(&alice, "steer-axfm", "hello from creator", &[])
                .is_ok()
        );
        assert!(
            store
                .post(&bob, "steer-axfm", "hello from member after join", &[])
                .is_ok()
        );
        assert!(
            store
                .post(&mallory, "steer-axfm", "intrude without join", &[])
                .is_err(),
            "non-member post must be refused"
        );
        // Idempotent re-join must not error and must keep membership.
        store
            .join("steer-axfm", &bob.public_key())
            .expect("re-join idempotent");
        assert!(store.is_member("steer-axfm", &bob.public_key()));
    }

    /// bd-e03q RED (ADR-0035 Spec 4): the store rebuilds from wire
    /// lifecycle events — a fresh process replays create + members and
    /// recovers the room, the single projection over the bus truth.
    #[test]
    fn rebuild_from_lifecycle_events_restores_membership() {
        let creator = keys();
        let member = keys();
        let create = build_event(
            &creator,
            toron_kind(CHANNEL_CREATE),
            "",
            vec![
                Tag::parse(["d", "rebuild-room"]).expect("d tag"),
                Tag::parse(["name", "Rebuild"]).expect("name tag"),
            ],
            Timestamp::now(),
        )
        .expect("create event");
        let members = build_event(
            &creator,
            toron_kind(CHANNEL_MEMBERS),
            serde_json::to_string(&vec![
                creator.public_key().to_hex(),
                member.public_key().to_hex(),
            ])
            .expect("members json"),
            vec![Tag::parse(["h", "rebuild-room"]).expect("h tag")],
            Timestamp::now(),
        )
        .expect("members event");
        // A fresh process, wiped cache: replay restores the room.
        let store = ChannelStore::rebuild([create, members]).expect("rebuild");
        assert!(
            store.is_member("rebuild-room", &member.public_key()),
            "replayed membership admits the member"
        );
        assert!(
            store
                .post(&member, "rebuild-room", "hello again", &[])
                .is_ok(),
            "rebuilt room accepts member posts"
        );
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
        let err = format!("{}", store.post(&author, "cap", &big, &[]).unwrap_err());
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
            .map(|c| c.split_once(' ').map(|x| x.1).expect("body"))
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
        assert_eq!(CHANNEL_CREATE, 39000, "ADR-0023 emit is kind 39000");
        assert_eq!(CHANNEL_MEMBERS, 39002);
        assert_eq!(CHANNEL_CREATE_LEGACY, 40);
        assert_eq!(CHANNEL_METADATA_LEGACY, 41);
        assert_eq!(CHANNEL_MESSAGE, 9);
        assert_eq!(CHANNEL_MESSAGE_LEGACY, 42);
        let has_t = ev.tags.iter().any(|t| format!("{t:?}").contains("toron"));
        assert!(has_t, "project slug on t tag, tags={:?}", ev.tags);
    }

    #[test]
    fn reaction_membership_gated() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        let mallory = keys();
        store
            .create_channel(&alice.public_key(), "c1", "C", false, None)
            .expect("create");
        store.join("c1", &bob.public_key()).expect("join bob");
        let post = store.post(&alice, "c1", "hello", &[]).expect("post");
        let alice_hex = alice.public_key().to_hex();

        let reaction = store
            .react(&bob, "c1", &post.id.to_hex(), "+1")
            .expect("member may react");
        assert_eq!(reaction.kind.as_u16(), REACTION);
        let parts: Vec<Vec<String>> = reaction
            .tags
            .iter()
            .map(|t| t.as_slice().iter().map(ToString::to_string).collect())
            .collect();
        assert!(
            parts
                .iter()
                .any(|t| t.first().map(String::as_str) == Some("p")
                    && t.get(1).map(String::as_str) == Some(alice_hex.as_str())),
            "NIP-25 p tag (target author) missing: {parts:?}"
        );
        assert!(
            parts
                .iter()
                .any(|t| t.first().map(String::as_str) == Some("k")
                    && t.get(1).map(String::as_str) == Some("9")),
            "NIP-25 k tag (target kind) missing: {parts:?}"
        );
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

    /// GS-062: leave runs Propose→Commit→FireLive(Kick) then mutates membership.
    #[test]
    fn leave_fires_kick_only_after_commit() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.join("c", &bob.public_key()).expect("join");
        store.leave("c", &bob.public_key()).expect("leave");
        assert!(!store.is_member("c", &bob.public_key()));
        // Ask-user A: authority held after FireLive — production join FenceReAdd.
        let err = store
            .join("c", &bob.public_key())
            .expect_err("re-join under held Kick must fence");
        assert!(err.to_string().contains("fenced"), "got {err}");
        store.settle_authority("c");
        store
            .join("c", &bob.public_key())
            .expect("re-join after settle_authority");
        assert!(store.is_member("c", &bob.public_key()));
    }

    /// GS-062: ReAdd under a held Kick generation is fenced at join
    /// (hold_authority_for_test helper; production path covered below).
    
    /// GS-062: apply_lifecycle leave settles authority so rebuild/replay does
    /// not permanently FenceReAdd. Live leave() still holds until settle_authority.
    #[test]
    fn apply_lifecycle_leave_settles_authority() {
        let alice = keys();
        let bob = keys();
        let create = build_event(
            &alice,
            toron_kind(CHANNEL_CREATE),
            "",
            vec![
                Tag::parse(["d", "c"]).expect("d"),
                Tag::parse(["name", "C"]).expect("name"),
            ],
            Timestamp::now(),
        )
        .expect("create");
        let members = build_event(
            &alice,
            toron_kind(CHANNEL_MEMBERS),
            serde_json::to_string(&vec![
                alice.public_key().to_hex(),
                bob.public_key().to_hex(),
            ])
            .expect("json"),
            vec![Tag::parse(["h", "c"]).expect("h")],
            Timestamp::now(),
        )
        .expect("members");
        let leave = build_event(
            &bob,
            toron_kind(CHANNEL_METADATA_LEGACY),
            "leave",
            vec![Tag::parse(["h", "c"]).expect("h")],
            Timestamp::now(),
        )
        .expect("leave");
        let store = ChannelStore::rebuild([create, members, leave]).expect("rebuild");
        assert!(!store.is_member("c", &bob.public_key()));
        // apply_lifecycle settled — join must NOT permanent-fence.
        store
            .join("c", &bob.public_key())
            .expect("join after apply_lifecycle leave must work (settled)");
        assert!(store.is_member("c", &bob.public_key()));
    }

    #[test]
    fn join_honors_readd_fence_under_held_kick() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.hold_authority_for_test(
            "c",
            Convergence::Committed {
                op: AuthorityOp::Kick {
                    target: bob.public_key().to_hex(),
                },
                generation: 7,
            },
        );
        let err = store
            .join("c", &bob.public_key())
            .expect_err("re-add under held kick must fence");
        assert!(
            err.to_string().contains("fenced"),
            "expected fence, got {err}"
        );
        assert!(!store.is_member("c", &bob.public_key()));
    }

    /// GS-062 ask-user A: leave holds authority — production join FenceReAdd
    /// without hold_authority_for_test.
    #[test]
    fn leave_holds_authority_join_fences_on_live_path() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.join("c", &bob.public_key()).expect("join");
        store.leave("c", &bob.public_key()).expect("leave");
        let err = store
            .join("c", &bob.public_key())
            .expect_err("live leave→rejoin must FenceReAdd while held");
        assert!(err.to_string().contains("fenced"), "got {err}");
        store.settle_authority("c");
        store.join("c", &bob.public_key()).expect("after settle");
    }

    /// GS-062 ask-user A hazard: other channels' leave/close bump the global
    /// authority_generation; join must still fence using held generation
    /// (not authority_generation+1 which can skip FenceReAdd).
    #[test]
    fn join_fences_despite_other_channel_generation_bumps() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        let carol = keys();
        store
            .create_channel(&alice.public_key(), "a", "A", false, None)
            .expect("a");
        store
            .create_channel(&alice.public_key(), "b", "B", false, None)
            .expect("b");
        store.join("a", &bob.public_key()).expect("bob a");
        store.join("b", &carol.public_key()).expect("carol b");
        store.leave("a", &bob.public_key()).expect("leave a — hold Kick");
        // Bump global authority_generation via other channel leave/close.
        store.leave("b", &carol.public_key()).expect("leave b");
        store
            .create_channel(&alice.public_key(), "d", "D", false, None)
            .expect("d");
        store.close("d", &alice.public_key()).expect("close d bumps gen");
        let err = store
            .join("a", &bob.public_key())
            .expect_err("must still FenceReAdd on a despite other-channel bumps");
        assert!(err.to_string().contains("fenced"), "got {err}");
    }

    /// GS-062: two mismatched joins under held Committed both FenceReAdd
    /// (fence must not be one-shot — Committed stays held).
    #[test]
    fn two_mismatched_joins_under_held_committed_both_fence() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        let carol = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.hold_authority_for_test(
            "c",
            Convergence::Committed {
                op: AuthorityOp::Kick {
                    target: bob.public_key().to_hex(),
                },
                generation: 7,
            },
        );
        let err1 = store
            .join("c", &bob.public_key())
            .expect_err("first re-add must fence");
        assert!(err1.to_string().contains("fenced"), "first: {err1}");
        let err2 = store
            .join("c", &carol.public_key())
            .expect_err("second re-add must still fence under held Committed");
        assert!(err2.to_string().contains("fenced"), "second: {err2}");
        assert!(!store.is_member("c", &bob.public_key()));
        assert!(!store.is_member("c", &carol.public_key()));
    }

    /// GS-062: close runs Propose→Commit→FireLive(Disable) then sets closed.
    #[test]
    fn close_fires_disable_only_after_commit() {
        let store = ChannelStore::new();
        let alice = keys();
        store
            .create_channel(&alice.public_key(), "c", "C", false, None)
            .expect("create");
        store.close("c", &alice.public_key()).expect("close");
        assert!(store.get("c").expect("ch").closed);
        let bob = keys();
        assert!(
            store.join("c", &bob.public_key()).is_err(),
            "closed channel refuses join"
        );
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

    /// bd-membership-gate-remedy-uti4: the gate error names the exact
    /// remedy — a bounce without the fix costs a help-page round-trip.
    #[test]
    fn membership_remedy_non_member_post_error_names_the_join_command() {
        let store = ChannelStore::new();
        let alice = keys();
        let mallory = keys();
        store
            .create_channel(&alice.public_key(), "steer", "Steer", true, Some("beads"))
            .expect("create");
        let err = format!(
            "{}",
            store.post(&mallory, "steer", "intrude", &[]).unwrap_err()
        );
        assert!(err.contains("not a member"), "{err}");
        assert!(
            err.contains("run: toron channel join steer --project beads --as <you>"),
            "remedy must be the exact join command: {err}"
        );
    }

    /// bd-membership-gate-remedy-uti4: a project-less channel still names
    /// the remedy, with a placeholder for the project.
    #[test]
    fn membership_remedy_projectless_channel_keeps_placeholder() {
        let store = ChannelStore::new();
        let alice = keys();
        let mallory = keys();
        store
            .create_channel(&alice.public_key(), "loose", "Loose", false, None)
            .expect("create");
        let err = format!(
            "{}",
            store.post(&mallory, "loose", "intrude", &[]).unwrap_err()
        );
        assert!(
            err.contains("run: toron channel join loose --project <p> --as <you>"),
            "{err}"
        );
    }

    /// bd-channel-close-purge-bsqb: close retires the channel — posts and
    /// joins are refused, the creator MAY leave (leave-rule fix), close is
    /// creator-only and idempotent.
    #[test]
    fn channel_close_retires_the_channel() {
        let store = ChannelStore::new();
        let alice = keys();
        let bob = keys();
        store
            .create_channel(&alice.public_key(), "probe", "Probe", false, Some("fw"))
            .expect("create");
        store.join("probe", &bob.public_key()).expect("bob joins");
        store
            .post(&alice, "probe", "one-off ordering probe", &[])
            .expect("pre-close post");

        // Only the creator may close.
        let err = store.close("probe", &bob.public_key()).unwrap_err();
        assert!(err.to_string().contains("creator"), "{err}");
        // Unknown channel: honest not-found, not a fake membership bounce.
        assert!(store.close("ghost", &alice.public_key()).is_err());

        store.close("probe", &alice.public_key()).expect("close");
        assert!(
            store.get("probe").expect("channel").closed,
            "closed flag is visible on the channel"
        );

        // Posts refused on a closed channel — even by a member.
        let err = store.post(&alice, "probe", "after close", &[]).unwrap_err();
        assert!(err.to_string().contains("closed"), "{err}");
        // Reactions refused too.
        assert!(
            store
                .react(&alice, "probe", "0".repeat(64).as_str(), "✅")
                .is_err()
        );
        // Joins refused: a retired channel accepts nobody new.
        assert!(
            store.join("probe", &keys().public_key()).is_err(),
            "join on a closed channel must fail"
        );
        // The creator MAY leave a closed channel (leave-rule fix).
        store
            .leave("probe", &alice.public_key())
            .expect("creator leaves a closed channel");
        assert!(!store.is_member("probe", &alice.public_key()));
        // Close is idempotent.
        store
            .close("probe", &alice.public_key())
            .expect("close again");

        // Pre-close history survives close (close retires, purge removes).
        assert!(!store.history("probe").is_empty());
    }
}
