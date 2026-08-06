// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! @mention dispatch (bd-yt2, beads-review: channel-agnostic).
//!
//! Reaching an agent = @mentioning it — on a kind-14 DM OR a kind-9 channel
//! message. The parser keys off a mention marker (`@name` in content) PLUS a
//! `p` tag carrying the mentioned agent's npub, so it works before channels
//! ship (P2.1) and after (P2.2 multi-party UX). A human DMs
//! `"@OCβ fix X"` → the dispatcher queues a kind-43001.

use nostr_sdk::nostr::{Event, PublicKey, TagKind};

/// A parsed mention: the target agent + the triggering message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mention {
    /// The mentioned agent's pubkey (from the `p` tag).
    pub target: PublicKey,
    /// The mentioned agent's display name as written (`@name`).
    pub name: String,
    /// The triggering message id (the job's parent `e`).
    pub message_id: nostr_sdk::nostr::EventId,
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
        if tag.kind() != TagKind::p() {
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
    use nostr_sdk::nostr::event::builder::Error;
    use nostr_sdk::nostr::{EventBuilder, JsonUtil, Keys, Tag};

    fn keys() -> Keys {
        Keys::generate()
    }

    fn event_with(content: &str, p_tags: &[String]) -> Result<Event, Error> {
        let k = keys();
        let tags: Vec<Tag> = p_tags
            .iter()
            .map(|n| Tag::parse(["p", n]).expect("p"))
            .collect();
        EventBuilder::new(nostr_sdk::nostr::Kind::TextNote, content)
            .tags(tags)
            .sign_with_keys(&k)
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
}
