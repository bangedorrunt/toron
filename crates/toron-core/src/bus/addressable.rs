// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Addressable / replaceable event semantics (NIP-33) + projection
//! key-priority (ADR-0003 A1).
//!
//! Kinds 30010–30014, 30031, 30032 are **parameterized-replaceable**: the
//! relay/bus dedup key is `(kind, pubkey, d)` — per author. A raw relay (or
//! bus) observer can see BOTH an agent's lease and an operator's revocation
//! event for the same `(kind, d)` because NIP-33 dedups per `(kind, pubkey,
//! d)`, never across authors.
//!
//! The **projection** applies an additional rule for toron semantics: for a
//! given `(kind, d)`, latest-wins with **key-priority operator > agent**.
//! That is what makes force-release (A1) and profile updates correct on
//! federated / multi-writer views: the operator's `status:revoked` event wins
//! over the agent's lease regardless of NIP-33 author isolation.

use crate::nostr::{Event, Kind, PublicKey};

/// Parameterized-replaceable kinds (NIP-33): 30000–39999.
pub const ADDRESSABLE_KIND_MIN: u16 = 30000;
/// Parameterized-replaceable kinds (NIP-33): 30000–39999.
pub const ADDRESSABLE_KIND_MAX: u16 = 39999;

/// True when `kind` is parameterized-replaceable (addressable).
pub fn is_addressable(kind: Kind) -> bool {
    let n = kind.as_u16();
    (ADDRESSABLE_KIND_MIN..=ADDRESSABLE_KIND_MAX).contains(&n)
}

/// The NIP-33 dedup key: `(kind, pubkey, d)`.
///
/// Returns `None` for non-addressable kinds or events lacking a `d` tag.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AddressableKey {
    /// Kind number.
    pub kind: u16,
    /// Author pubkey (hex).
    pub pubkey: String,
    /// The `d` tag value.
    pub d: String,
}

impl AddressableKey {
    /// Extract the key from an event, if addressable.
    pub fn from_event(event: &Event) -> Option<Self> {
        if !is_addressable(event.kind) {
            return None;
        }
        let d = event
            .tags
            .iter()
            .find_map(|t| if t.kind() == "d" { t.content() } else { None })?;
        Some(Self {
            kind: event.kind.as_u16(),
            pubkey: event.pubkey.to_hex(),
            d: d.to_string(),
        })
    }
}

/// Marker: the event is a revocation (`status:revoked`) — the A1 force-release
/// signal. Recognized forms: plain `status:revoked` in content, JSON
/// `"status":"revoked"` in content, or a `["status", "revoked"]` tag.
pub fn is_revocation(event: &Event) -> bool {
    event.content.contains("status:revoked")
        || event.content.contains("\"status\":\"revoked\"")
        || event
            .tags
            .iter()
            .any(|t| t.as_slice().windows(2).any(|w| w == ["status", "revoked"]))
}

/// Priority class of an author for projection resolution (A1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyPriority {
    /// Agent / ordinary actor.
    Agent = 0,
    /// The toron operator (revocation authority).
    Operator = 1,
}

impl KeyPriority {
    /// Priority of an event's author given the operator key.
    ///
    /// The operator's own key is always `Operator`; every other pubkey is an
    /// `Agent`.
    pub fn of(event: &Event, operator: Option<&PublicKey>) -> Self {
        match operator {
            Some(op) if &event.pubkey == op => Self::Operator,
            _ => Self::Agent,
        }
    }
}

/// Resolve a set of events sharing the same `(kind, d)` coordinate into the
/// single toron-visible state (projection key-priority rule).
///
/// - Group by author; within an author, latest-wins (NIP-33).
/// - Across authors: an operator event outranks any agent event (A1).
/// - Among the highest-priority class, latest `created_at` wins; ties break
///   on the canonical `event_id`.
///
/// A raw relay observer sees all events; toron tools see only this resolved
/// one. Returns `None` when no event matches (e.g. only revocations for a
/// non-existent lease).
pub fn resolve_coordinate<'a>(
    events: impl IntoIterator<Item = &'a Event>,
    operator: Option<&PublicKey>,
) -> Option<Event> {
    let events: Vec<&Event> = events.into_iter().collect();
    if events.is_empty() {
        return None;
    }

    // Highest-priority class wins.
    let best_class = events
        .iter()
        .map(|e| KeyPriority::of(e, operator))
        .max()
        .unwrap_or(KeyPriority::Agent);

    // Among the best class, NIP-01 latest-wins (created_at desc, lowest id).
    events
        .into_iter()
        .filter(|e| KeyPriority::of(e, operator) == best_class)
        .max_by(|a, b| nip01_replaceable_cmp(a, b))
        .cloned()
}

/// NIP-01 replaceable/addressable order: higher `created_at` wins; on a
/// timestamp tie, the lexicographically lowest event id wins.
pub fn nip01_replaceable_cmp(a: &Event, b: &Event) -> std::cmp::Ordering {
    a.created_at
        .as_secs()
        .cmp(&b.created_at.as_secs())
        .then_with(|| b.id.to_hex().cmp(&a.id.to_hex()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nostr::event::build_event;
    use crate::nostr::kinds::{FILE_RESERVATION, PROJECT_MANIFEST};
    use crate::nostr::{Keys, Tag, Timestamp};

    fn reservation(keys: &Keys, d: &str, content: &str, ts: u64) -> Event {
        build_event(
            keys,
            crate::nostr::kinds::toron_kind(FILE_RESERVATION),
            content,
            vec![Tag::parse(["d", d]).expect("d")],
            Timestamp::from(ts),
        )
        .expect("build")
    }

    #[test]
    fn test_addressable_kind_range() {
        assert!(is_addressable(crate::nostr::kinds::toron_kind(
            FILE_RESERVATION
        )));
        assert!(is_addressable(crate::nostr::kinds::toron_kind(
            PROJECT_MANIFEST
        )));
        assert!(!is_addressable(crate::nostr::Kind::TextNote));
        assert!(!is_addressable(crate::nostr::Kind::GiftWrap));
    }

    #[test]
    fn test_addressable_key_from_event() {
        let keys = Keys::generate();
        let ev = reservation(&keys, "proj:src/**", "lease", 1_700_000_000);
        let key = AddressableKey::from_event(&ev).expect("addressable");
        assert_eq!(key.kind, FILE_RESERVATION);
        assert_eq!(key.pubkey, keys.public_key().to_hex());
        assert_eq!(key.d, "proj:src/**");

        // Non-addressable → None.
        let note = build_event(
            &keys,
            crate::nostr::Kind::TextNote,
            "hi",
            Vec::<Tag>::new(),
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build");
        assert!(AddressableKey::from_event(&note).is_none());
    }

    /// Same d, same pubkey → later event wins (NIP-33 author isolation).
    #[test]
    fn test_latest_wins_same_author() {
        let agent = Keys::generate();
        let ev1 = reservation(&agent, "proj:src/**", "lease v1", 1_700_000_000);
        let ev2 = reservation(&agent, "proj:src/**", "lease v2", 1_700_000_001);
        let resolved = resolve_coordinate([&ev1, &ev2], None).expect("resolved");
        assert_eq!(resolved.id, ev2.id, "later event wins for same author");
    }

    /// Same d, different pubkeys, no operator → latest agent wins.
    #[test]
    fn test_latest_wins_different_authors_no_operator() {
        let agent_a = Keys::generate();
        let agent_b = Keys::generate();
        let ev_a = reservation(&agent_a, "proj:src/**", "from a", 1_700_000_000);
        let ev_b = reservation(&agent_b, "proj:src/**", "from b", 1_700_000_001);
        let resolved = resolve_coordinate([&ev_a, &ev_b], None).expect("resolved");
        assert_eq!(resolved.id, ev_b.id, "later event wins without operator");
    }

    /// Same d, operator revocation + agent lease → operator wins in the
    /// projection (A1), even though the agent lease is newer.
    #[test]
    fn test_operator_revoke_wins_over_agent_lease() {
        let agent = Keys::generate();
        let operator = Keys::generate();

        let lease = reservation(&agent, "proj:src/**", "agent lease", 1_700_000_002);
        let revoke = reservation(
            &operator,
            "proj:src/**",
            "{\"status\":\"revoked\"}",
            1_700_000_001,
        );
        assert!(is_revocation(&revoke));
        assert!(!is_revocation(&lease));

        let resolved =
            resolve_coordinate([&lease, &revoke], Some(&operator.public_key())).expect("resolved");
        assert_eq!(resolved.id, revoke.id, "operator revocation wins (A1)");
        assert!(is_revocation(&resolved));
    }

    /// Without the operator key configured, the same revocation is just an
    /// agent — latest wins (raw relay view parity).
    #[test]
    fn test_no_operator_key_means_no_priority() {
        let agent = Keys::generate();
        let operator = Keys::generate();
        let lease = reservation(&agent, "proj:src/**", "agent lease", 1_700_000_002);
        let revoke = reservation(
            &operator,
            "proj:src/**",
            "{\"status\":\"revoked\"}",
            1_700_000_001,
        );
        // operator=None → both are Agents; the newer lease wins.
        let resolved = resolve_coordinate([&lease, &revoke], None).expect("resolved");
        assert_eq!(resolved.id, lease.id);
    }

    /// The revocation flag is content- or tag-driven.
    #[test]
    fn test_is_revocation_detection() {
        let keys = Keys::generate();
        let content_revoke = reservation(&keys, "p:s", "status:revoked", 1_700_000_000);
        assert!(is_revocation(&content_revoke));

        let tag_revoke = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(FILE_RESERVATION),
            "lease",
            vec![
                Tag::parse(["d", "p:s"]).expect("d"),
                Tag::parse(["status", "revoked"]).expect("status"),
            ],
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build");
        assert!(is_revocation(&tag_revoke));

        let plain = reservation(&keys, "p:s", "lease", 1_700_000_000);
        assert!(!is_revocation(&plain));
    }

    /// Empty input resolves to None.
    #[test]
    fn test_resolve_empty() {
        assert!(resolve_coordinate(Vec::<&Event>::new(), None).is_none());
    }

    /// NIP-01: equal `created_at` keeps the lowest id (lexicographic).
    #[test]
    fn test_equal_created_at_keeps_lowest_id() {
        let agent = Keys::generate();
        let a = reservation(&agent, "proj:src/**", "lease-a", 1_700_000_000);
        let b = reservation(&agent, "proj:src/**", "lease-b", 1_700_000_000);
        assert_ne!(a.id, b.id);
        let (low, high) = if a.id.to_hex() < b.id.to_hex() {
            (a, b)
        } else {
            (b, a)
        };
        let resolved = resolve_coordinate([&high, &low], None).expect("resolved");
        assert_eq!(
            resolved.id,
            low.id,
            "NIP-01 tie-break: lowest id wins, got {}",
            resolved.id.to_hex()
        );
        let resolved_rev = resolve_coordinate([&low, &high], None).expect("resolved");
        assert_eq!(resolved_rev.id, low.id, "order-independent lowest id");
    }
}
