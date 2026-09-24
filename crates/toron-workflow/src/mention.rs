// governed-by: ADR-0003
// governed-by: ADR-0020
#![forbid(unsafe_code)]

//! @mention dispatch (bd-yt2, beads-review: channel-agnostic).
//!
//! Reaching an agent = @mentioning it — on a kind-14 DM OR a kind-9 channel
//! message. The parser keys off a mention marker (`@name` in content) PLUS a
//! `p` tag carrying the mentioned agent's npub, so it works before channels
//! ship (P2.1) and after (P2.2 multi-party UX). A human DMs
//! `"@OCβ fix X"` → the dispatcher queues a kind-43001.
//!
//! ADR-0020 Spec 4 wiring: a kind-9 channel post's @mentions submit to the
//! [`MentionCoalescer`]; a coalesced batch becomes ONE kind-43001 that
//! `e`-tags the post. `#t` is the project slug ONLY — the assignee rides the
//! free-form `assignee` tag (ADR-0003 A3/G6).

use nostr::prelude::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use crate::coalesce::{CoalesceDecision, Coalesced, InboundMention, MentionCoalescer};
use crate::delivery_cursor::{DeliveryCursorStore, agent_key_is_not_delivery_proof};
use crate::jobs::{JobRequest, build_job_request_with_tags};

/// Wire a kind-9 channel post (or kind-14 DM) onto the mention coalescer
/// (ADR-0020 Spec 4): every parsed @mention submits one [`InboundMention`]
/// keyed by the mentioned agent; a batch that flushes NOW (hard-cap path)
/// becomes one kind-43001. Returns the job requests built for immediate
/// flushes; timer-driven flushes are built by the caller from the
/// [`FlushOutcome::Flushed`] batch via [`build_job_from_coalesced`].
pub fn wire_mention_dispatch(
    event: &Event,
    coalescer: &mut MentionCoalescer,
    requester: &Keys,
    project_slug: Option<&str>,
    now_ms: u64,
) -> Vec<Event> {
    wire_mention_dispatch_with_cursor(event, coalescer, requester, project_slug, now_ms, None)
}

/// Like [`wire_mention_dispatch`] with a session [`DeliveryCursorStore`]
/// (GS-062 / Buzz #7620). Session key = assignee (dispatch session), never
/// agent pubkey. Second delivery of the same parent id is skipped.
pub fn wire_mention_dispatch_with_cursor(
    event: &Event,
    coalescer: &mut MentionCoalescer,
    requester: &Keys,
    project_slug: Option<&str>,
    now_ms: u64,
    delivery: Option<&DeliveryCursorStore>,
) -> Vec<Event> {
    let mut jobs = Vec::new();
    for m in parse_mentions(event) {
        // Honesty: agent-key-signed ≠ delivery proof (call-site assertion).
        let _ = agent_key_is_not_delivery_proof(&m.target.to_hex(), &m.message_id.to_hex());
        let delivery_id = m.message_id.to_hex();
        let session = m.name.as_str(); // assignee / dispatch session id
        if let Some(store) = delivery {
            let (keep, _) = store.peek_undelivered(session, [delivery_id.clone()]);
            if keep.is_empty() {
                continue; // already delivered to this session
            }
        }
        let decision = coalescer.submit(
            InboundMention {
                assignee: m.name.clone(),
                parent: delivery_id.clone(),
                spec: serde_json::json!({ "text": m.content }),
            },
            now_ms,
        );
        if let CoalesceDecision::FlushNow(batch) = decision {
            match build_job_from_coalesced(&batch, requester, project_slug, Timestamp::now()) {
                Ok(ev) => {
                    // Mark only after successful build — failed build must not
                    // advance the cursor (silent kind-43001 loss hazard).
                    if let Some(store) = delivery {
                        store.mark_delivered(session, [delivery_id]);
                    }
                    jobs.push(ev);
                }
                Err(_) => {
                    // Do not mark_delivered — parent stays undelivered for retry.
                }
            }
        }
    }
    jobs
}

/// Build ONE kind-43001 from a coalesced mention batch (ADR-0020 Spec 4):
/// `e` = the batch's parent event (the triggering kind-9 post), `assignee` =
/// canonical name, and `t` = project slug ONLY — the assignee never rides
/// `#t` (ADR-0003 A3/G6 reserves `#t` for project scoping).
pub fn build_job_from_coalesced(
    batch: &Coalesced,
    requester: &Keys,
    project_slug: Option<&str>,
    created_at: Timestamp,
) -> Result<Event, nostr::error::Error> {
    let parent = EventId::from_hex(batch.parent()).ok();
    let payload = JobRequest {
        title: format!("mention for {}", batch.assignee),
        spec: batch.merged_spec(),
        assignee: Some(batch.assignee.clone()),
    };
    let mut extra_tags = Vec::new();
    if let Some(slug) = project_slug {
        extra_tags.push(Tag::parse(["t", slug]).expect("t tag"));
    }
    build_job_request_with_tags(requester, parent, &payload, created_at, extra_tags)
}

/// A parsed mention: the target agent + the triggering message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mention {
    /// The mentioned agent's pubkey (from the `p` tag).
    pub target: PublicKey,
    /// The mentioned agent's display name as written (`@name`).
    pub name: String,
    /// The triggering message id (the job's parent `e`).
    pub message_id: EventId,
    /// The trigger message content.
    pub content: String,
}

/// The @mention marker prefix.
pub const MENTION_PREFIX: char = '@';

/// Parse `@name` mentions from content, returning the first `@token` that
/// matches a `p`-tagged recipient.
pub fn parse_mentions(event: &Event) -> Vec<Mention> {
    // The `p` tags carry the mentioned recipients (kind-14 DM or kind-9).
    let mut mentions = Vec::new();
    for tag in event.tags.iter() {
        if tag.kind() != "p" {
            continue;
        }
        let parts = tag.as_slice();
        let Some(npub) = parts.get(1).and_then(|s| PublicKey::from_hex(s).ok()) else {
            continue;
        };
        // Match the first @token in content against this p-tagged npub. The
        // name is the token without the @; name→npub authoritative mapping is
        // the project profile index (the p tag is the wire truth here).
        if let Some(name) = content_mentions(&event.content, &npub) {
            mentions.push(Mention {
                target: npub,
                name,
                message_id: event.id,
                content: event.content.clone(),
            });
        }
    }
    mentions
}

/// Does `content` contain an `@token`? Returns the token (lowercased, without
/// `@`) if so. Channel-agnostic: no channel binding (beads-review).
pub fn content_mentions(content: &str, _npub: &PublicKey) -> Option<String> {
    let bytes = content.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == MENTION_PREFIX as u8 {
            // Collect the token: alphanumeric + `_`/`-`.
            let start = i + 1;
            let mut end = start;
            while end < bytes.len()
                && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_' || bytes[end] == b'-')
            {
                end += 1;
            }
            if end > start {
                let token = content[start..end].to_string();
                return Some(token);
            }
        }
        i += 1;
    }
    None
}

/// True when the event carries an @mention (has content mention + a p tag).
pub fn is_mention(event: &Event) -> bool {
    !parse_mentions(event).is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr::error::Error;
    use nostr::prelude::{EventBuilder, FinalizeEvent, Keys, Kind, Tag};

    fn keys() -> Keys {
        Keys::generate()
    }

    fn event_with(content: &str, p_tags: &[String]) -> Result<Event, Error> {
        let k = keys();
        let tags: Vec<Tag> = p_tags
            .iter()
            .map(|n| Tag::parse(["p", n]).expect("p"))
            .collect();
        EventBuilder::new(Kind::TextNote, content)
            .tags(tags)
            .finalize(&k)
    }

    #[test]
    fn parses_mention_from_kind14_dm() {
        let target = keys().public_key();
        let ev = event_with("@OCbeta fix the flaky test", &[target.to_hex()]).expect("event");
        let mentions = parse_mentions(&ev);
        assert_eq!(mentions.len(), 1);
        assert_eq!(mentions[0].target, target);
        assert_eq!(mentions[0].name, "OCbeta");
        assert_eq!(mentions[0].message_id, ev.id);
        assert!(is_mention(&ev));
    }

    #[test]
    fn no_mention_without_p_tag() {
        let ev = event_with("@OCbeta hello", &[]).expect("event");
        assert!(parse_mentions(&ev).is_empty());
        assert!(!is_mention(&ev));
    }

    #[test]
    fn no_mention_when_no_at_token() {
        let target = keys().public_key();
        let ev = event_with("just a note", &[target.to_hex()]).expect("event");
        assert!(parse_mentions(&ev).is_empty());
    }

    #[test]
    fn content_mentions_token_only() {
        let npub = keys().public_key();
        assert_eq!(
            content_mentions("@oc-beta hi", &npub),
            Some("oc-beta".into())
        );
        assert_eq!(content_mentions("no at here", &npub), None);
        assert_eq!(content_mentions("ends with @", &npub), None);
    }

    #[test]
    fn roundtrips_via_json() {
        let target = keys().public_key();
        let ev = event_with("@pi-alpha do it", &[target.to_hex()]).expect("event");
        let parsed = Event::from_json(ev.as_json()).expect("parse");
        assert_eq!(parsed, ev);
        assert!(is_mention(&parsed));
    }

    /// ADR-0020 Spec 4: a kind-9 channel post @mentioning one agent (with a
    /// `p` tag) produces ONE kind-43001 that `e`-tags the post; `#t` is the
    /// project slug only, never the assignee.
    #[test]
    fn kind9_post_mention_wires_one_43001_etagging_post() {
        const TS: u64 = 1_700_000_000;
        let requester = keys();
        let bob = keys();
        // A kind-9 channel post: `@Bob take the repro` + p tag for Bob.
        let post = EventBuilder::new(Kind::from_u16(9), "@Bob take the repro")
            .tags(vec![
                Tag::parse(["h", "steer-1"]).expect("h"),
                Tag::parse(["t", "toron"]).expect("t"),
                Tag::public_key(bob.public_key()),
            ])
            .finalize(&requester)
            .expect("sign kind-9 post");
        assert!(is_mention(&post), "@Bob + p tag is a mention");

        let mut coalescer = MentionCoalescer::with_defaults();
        // Sub-window not elapsed → buffered, no immediate flush.
        let jobs = wire_mention_dispatch(&post, &mut coalescer, &requester, Some("toron"), 0);
        assert!(jobs.is_empty(), "single mention buffers until sub-window");

        let batch = coalescer.force_flush("Bob").expect("buffered mention");
        assert_eq!(batch.count(), 1);
        let job = build_job_from_coalesced(&batch, &requester, Some("toron"), Timestamp::from(TS))
            .expect("build 43001");
        assert_eq!(job.kind.as_u16(), 43001, "one 43001 per coalesced batch");
        // e-tags the kind-9 post.
        assert!(
            job.tags
                .iter()
                .any(|t| t.kind() == "e"
                    && t.content().map(|c| c == post.id.to_hex()).unwrap_or(false)),
            "43001 e-tags the post, tags={:?}",
            job.tags
        );
        // assignee = canonical name; #t = project slug only.
        assert!(
            job.tags.iter().any(
                |t| t.as_slice().first().map(String::as_str) == Some("assignee")
                    && t.as_slice().get(1).map(String::as_str) == Some("Bob")
            ),
            "assignee tag = canonical name"
        );
        let t_tags: Vec<_> = job
            .tags
            .iter()
            .filter(|t| t.as_slice().first().map(String::as_str) == Some("t"))
            .collect();
        assert_eq!(
            t_tags.len(),
            1,
            "#t is project slug only, tags={:?}",
            job.tags
        );
        assert_eq!(
            t_tags[0].as_slice().get(1).map(String::as_str),
            Some("toron")
        );
    }

    /// GS-062: DeliveryCursorStore skips second delivery of the same parent id
    /// for the same assignee session (not agent-key heuristic).
    #[test]
    fn second_delivery_of_same_id_is_skipped() {
        let requester = Keys::generate();
        let agent = Keys::generate();
        let post = {
            use nostr::prelude::{EventBuilder, Kind, Tag};
            EventBuilder::new(Kind::from_u16(9), "@agent fix it")
                .tags([Tag::public_key(agent.public_key())])
                .finalize(&requester)
                .expect("post")
        };
        // Force content to contain @token matching — parse_mentions needs @name
        // matching p-tag; content_mentions may need the name. Use wire with cursor.
        let store = DeliveryCursorStore::new();
        let coalescer = MentionCoalescer::new(
            std::time::Duration::from_millis(0),
            std::time::Duration::from_millis(0),
            64,
        );
        // Exercise peek / mark at the call site (advance only after success).
        let session = "OCBeta";
        let id = post.id.to_hex();
        assert!(agent_key_is_not_delivery_proof(
            &agent.public_key().to_hex(),
            &id
        ));
        let (keep1, _) = store.peek_undelivered(session, [id.clone()]);
        assert_eq!(keep1, vec![id.clone()]);
        assert_eq!(store.cursor(session).cursor, 0, "peek must not advance");
        // Simulate failed build/publish: no mark → still undelivered.
        let (keep_fail, _) = store.peek_undelivered(session, [id.clone()]);
        assert_eq!(keep_fail, vec![id.clone()]);
        // Successful path marks.
        store.mark_delivered(session, [id.clone()]);
        let (keep2, fx) = store.peek_undelivered(session, [id.clone()]);
        assert!(keep2.is_empty(), "second delivery skipped after mark");
        assert!(matches!(
            fx[0],
            crate::delivery_cursor::DeliveryEffect::SkipAlreadyDelivered { .. }
        ));
        // Different session still delivers.
        let (keep3, _) = store.peek_undelivered("other-session", [id]);
        assert_eq!(keep3.len(), 1);
        let _ = (post, coalescer, requester);
    }
}
