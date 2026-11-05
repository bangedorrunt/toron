// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Dual-emit project scoping tags (ADR-0003 A3).
//!
//! Every **non-encrypted** event that can cross a relay carries BOTH:
//! - `["t", <slug>]` — cheap InProcessBus indexed shortcut;
//! - NIP-32 labels `["L", "toron"]` + `["l", <slug>, "toron"]` — namespaced,
//!   collision-free on shared relays.
//!
//! **Gift-wrapped / private DM events (kind-1059, seal kind-13, and the
//! wrapped rumor) carry NEITHER `#t` NOR NIP-32 labels** — project membership
//! would leak in the envelope metadata (ADR-0003 G6/A3 metadata-hiding). Those
//! events are project-scoped implicitly by recipient membership at the
//! projection.

use crate::nostr::{Kind, Tag};

use crate::bus::filter::LABEL_NAMESPACE;

/// Project scoping tags for a non-encrypted, relay-crossing event.
///
/// Returns the `#t` shortcut + the NIP-32 namespace/label pair.
pub fn project_scoping_tags(slug: &str) -> Vec<Tag> {
    vec![
        Tag::parse(["t", slug]).expect("t tag"),
        Tag::parse(["L", LABEL_NAMESPACE]).expect("L tag"),
        Tag::parse(["l", slug, LABEL_NAMESPACE]).expect("l tag"),
    ]
}

/// True when a kind must NEVER carry project-scoping tags (metadata hiding).
///
/// Wraps (1059), seals (13), and private DMs (14/15) are encrypted; project
/// scoping leaks the project in envelope metadata. ADR-0003: gift-wrapped DMs
/// carry neither `#t` nor NIP-32 labels.
pub const fn is_metadata_hiding_kind(kind: Kind) -> bool {
    matches!(kind.as_u16(), 13 | 14 | 15 | 1059)
}

/// True when a kind is in the **ephemeral class** (20000–29999, ADR-0003):
/// presence (20001), typing, agent working-state. Ephemeral events are
/// broadcast on the bus but NEVER stored in SQLite or written to Git — a
/// missed event just means stale state until the next one.
pub fn is_ephemeral_kind(kind: Kind) -> bool {
    let n = kind.as_u16();
    (20000..=29999).contains(&n)
}

/// Append project-scoping tags to an event tag list **unless** the kind is
/// metadata-hiding. Used by the bus publish path so callers get the A3 rule
/// for free.
pub fn with_project_scoping(
    kind: Kind,
    base_tags: impl IntoIterator<Item = Tag>,
    slug: Option<&str>,
) -> Vec<Tag> {
    let mut tags: Vec<Tag> = base_tags.into_iter().collect();
    if let Some(slug) = slug
        && !is_metadata_hiding_kind(kind)
    {
        tags.extend(project_scoping_tags(slug));
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::gift_wrap;
    use crate::nostr::kinds::{AGENT_PROFILE, FILE_RESERVATION, PROJECT_MANIFEST};
    use crate::nostr::{Keys, Timestamp};

    fn has_tag(ev: &crate::nostr::Event, needle: &[&str]) -> bool {
        ev.tags.iter().any(|t| {
            let parts: Vec<&str> = t.as_slice().iter().map(String::as_str).collect();
            parts == needle
        })
    }

    #[test]
    fn test_dual_tags_present_on_agent_profile() {
        let keys = Keys::generate();
        let tags = with_project_scoping(
            crate::nostr::kinds::toron_kind(AGENT_PROFILE),
            vec![Tag::parse(["d", "demo:BlueLake"]).expect("d")],
            Some("demo"),
        );
        let ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(AGENT_PROFILE),
            "profile",
            tags,
            Timestamp::now(),
        )
        .expect("build");

        assert!(has_tag(&ev, &["t", "demo"]), "must carry #t shortcut");
        assert!(has_tag(&ev, &["L", "toron"]), "must carry NIP-32 namespace");
        assert!(
            has_tag(&ev, &["l", "demo", "toron"]),
            "must carry NIP-32 label"
        );
        assert!(has_tag(&ev, &["d", "demo:BlueLake"]));
    }

    #[test]
    fn test_dual_tags_present_on_file_reservation() {
        let keys = Keys::generate();
        let tags = with_project_scoping(
            crate::nostr::kinds::toron_kind(FILE_RESERVATION),
            vec![
                Tag::parse(["d", "demo:src/**"]).expect("d"),
                Tag::expiration(Timestamp::from(1_800_000_000_u64)),
            ],
            Some("demo"),
        );
        let ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(FILE_RESERVATION),
            "lease",
            tags,
            Timestamp::now(),
        )
        .expect("build");

        assert!(has_tag(&ev, &["t", "demo"]));
        assert!(has_tag(&ev, &["l", "demo", "toron"]));
    }

    #[test]
    fn test_project_manifest_dual_tags() {
        let keys = Keys::generate();
        let tags = with_project_scoping(
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            vec![Tag::parse(["d", "demo"]).expect("d")],
            Some("demo"),
        );
        let ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "{}",
            tags,
            Timestamp::now(),
        )
        .expect("build");
        assert!(has_tag(&ev, &["t", "demo"]));
        assert!(has_tag(&ev, &["l", "demo", "toron"]));
    }

    #[tokio::test]
    async fn test_no_project_tags_on_gift_wrap() {
        let sender = Keys::generate();
        let recipient = Keys::generate();

        let rumor = gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000_u64),
            Kind::PrivateDirectMessage,
            vec![Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p")],
            "secret",
        );
        let wrapped = gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        assert!(!has_tag(&wrapped, &["t", "demo"]));
        assert!(!has_tag(&wrapped, &["L", "toron"]));
        assert!(!has_tag(&wrapped, &["l", "demo", "toron"]));
        assert_eq!(wrapped.kind.as_u16(), 1059);
    }

    #[tokio::test]
    async fn test_wrap_rumor_has_no_project_tags_either() {
        let sender = Keys::generate();
        let recipient = Keys::generate();
        let rumor = gift_wrap::build_rumor(
            sender.public_key(),
            Timestamp::from(1_700_000_000_u64),
            Kind::PrivateDirectMessage,
            vec![
                Tag::parse(["p", &recipient.public_key().to_hex()]).expect("p"),
                Tag::parse(["subject", "review"]).expect("subject"),
            ],
            "body",
        );
        let wrapped = gift_wrap::gift_wrap(&sender, &recipient.public_key(), rumor).expect("wrap");

        let envelope_tags: Vec<Vec<String>> = wrapped
            .tags
            .iter()
            .map(|t| t.as_slice().iter().map(ToString::to_string).collect())
            .collect();
        assert_eq!(envelope_tags.len(), 1, "got {envelope_tags:?}");
        assert_eq!(envelope_tags[0][0], "p");
    }

    #[test]
    fn test_metadata_hiding_kinds_identified() {
        assert!(is_metadata_hiding_kind(Kind::GiftWrap));
        assert!(is_metadata_hiding_kind(Kind::Seal));
        assert!(is_metadata_hiding_kind(Kind::PrivateDirectMessage));
        assert!(is_metadata_hiding_kind(Kind::from_u16(15)));
        assert!(!is_metadata_hiding_kind(crate::nostr::kinds::toron_kind(
            AGENT_PROFILE
        )));
        assert!(!is_metadata_hiding_kind(crate::nostr::kinds::toron_kind(
            PROJECT_MANIFEST
        )));
    }

    #[test]
    fn test_ephemeral_kinds_identified() {
        assert!(is_ephemeral_kind(crate::nostr::kinds::toron_kind(
            crate::nostr::kinds::PRESENCE
        )));
        assert!(is_ephemeral_kind(Kind::from_u16(20001)));
        assert!(is_ephemeral_kind(Kind::from_u16(29999)));
        assert!(!is_ephemeral_kind(Kind::from_u16(19999)));
        assert!(!is_ephemeral_kind(Kind::from_u16(30000)));
        assert!(!is_ephemeral_kind(crate::nostr::kinds::toron_kind(
            AGENT_PROFILE
        )));
        assert!(!is_ephemeral_kind(Kind::GiftWrap));
    }

    #[test]
    fn test_with_project_scoping_skips_hiding_kinds() {
        let base: Vec<Tag> = vec![];
        let scoped = with_project_scoping(Kind::GiftWrap, base.clone(), Some("demo"));
        assert!(
            scoped.is_empty(),
            "metadata-hiding kinds get no project tags"
        );

        let scoped_dm =
            with_project_scoping(Kind::PrivateDirectMessage, base.clone(), Some("demo"));
        assert!(scoped_dm.is_empty());

        let scoped_plain = with_project_scoping(Kind::TextNote, base, Some("demo"));
        assert_eq!(scoped_plain.len(), 3, "non-hiding kinds get 3 scoping tags");
    }

    #[test]
    fn test_no_slug_means_no_scoping() {
        let base: Vec<Tag> = vec![];
        assert!(with_project_scoping(Kind::TextNote, base, None).is_empty());
    }
}
