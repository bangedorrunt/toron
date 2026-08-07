// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Ephemeral presence (kind-20001) — agent working-state heartbeat.
//!
//! ADR-0003: presence/working-state is **ephemeral (not stored)** — broadcast
//! on the bus, never projected to SQLite or Git (ephemeral class 20000–29999,
//! Buzz interop). A missed heartbeat means stale presence until the next one;
//! consumers tolerate gaps via "last heartbeat wins + expiry on stale".
//!
//! This module is the pure event-shape half: build + parse kind-20001 events.
//! The live broker (broadcast fan-out + expiry) lives in `bus::presence`.

use nostr_sdk::nostr::event::builder::Error as BuilderError;
use nostr_sdk::nostr::{Event, EventId, Keys, PublicKey, Tag, Timestamp};

use crate::nostr::event::build_event;
use crate::nostr::kinds::{PRESENCE, toron_kind};

/// Agent working state (mirrors the herdr `AgentStatus` vocabulary so presence
/// can replace herdr-polling — ADR-0003 "replaces herdr-polling for presence").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PresenceState {
    /// Agent is idle / awaiting work.
    Idle,
    /// Agent is actively working.
    Working,
    /// Agent is blocked waiting on something.
    Blocked,
    /// Agent finished its current task.
    Done,
    /// Unknown / no heartbeat yet.
    #[default]
    Unknown,
}

/// Presence heartbeat payload (kind-20001 `content`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PresencePayload {
    /// Working state.
    pub state: PresenceState,
    /// Optional human/agent status message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    /// Heartbeat time (unix seconds).
    pub ts: u64,
}

/// Build a kind-20001 presence heartbeat.
///
/// Ephemeral — no NIP-40 expiration tag needed (it is never stored); the
/// broker expires presence by staleness. Presence is a **system-level** signal
/// (replaces herdr-polling: every agent's working state is visible to any
/// consumer), so it deliberately carries NO project scoping (`#t`/NIP-32) —
/// a global bus subscription (the broker) must receive it, and the access
/// gate only restricts project-scoped events. Project context, if any, lives
/// in the payload's `project` field.
pub fn build_presence_event(
    keys: &Keys,
    payload: &PresencePayload,
    created_at: Timestamp,
) -> Result<Event, BuilderError> {
    let content = serde_json::to_string(payload).expect("presence serializes");
    build_event(
        keys,
        toron_kind(PRESENCE),
        content,
        Vec::<Tag>::new(),
        created_at,
    )
}

/// Parse a kind-20001 event back into its payload.
pub fn parse_presence(event: &Event) -> Option<PresencePayload> {
    if event.kind.as_u16() != PRESENCE {
        return None;
    }
    serde_json::from_str(&event.content).ok()
}

/// Convenience: event id as hex (diagnostics / SC-15 assertions).
pub fn presence_event_id(event: &Event) -> EventId {
    event.id
}

/// Convenience for tests / SC-15: the author of a presence event.
pub fn presence_author(event: &Event) -> PublicKey {
    event.pubkey
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::nostr::JsonUtil;

    #[test]
    fn test_presence_state_serde() {
        let s = PresenceState::Working;
        let json = serde_json::to_string(&s).expect("json");
        assert_eq!(json, "\"working\"");
        let back: PresenceState = serde_json::from_str(&json).expect("parse");
        assert_eq!(back, PresenceState::Working);
        assert_eq!(PresenceState::default(), PresenceState::Unknown);
    }

    #[test]
    fn test_presence_payload_roundtrip() {
        let keys = Keys::generate();
        let payload = PresencePayload {
            state: PresenceState::Blocked,
            status: Some("waiting on herdr".into()),
            ts: 1_700_000_000,
        };
        let ev = build_presence_event(&keys, &payload, Timestamp::from(1_700_000_000_u64))
            .expect("build");
        assert_eq!(ev.kind.as_u16(), PRESENCE);
        // System-level: NO project scoping (global broker sub must receive it).
        assert!(
            !ev.tags
                .iter()
                .any(|t| t.as_slice() == ["t".to_string(), "demo".to_string()]),
            "presence must not carry project #t scoping"
        );
        // Not metadata-hiding.
        assert!(!crate::bus::is_metadata_hiding_kind(ev.kind));

        let parsed = parse_presence(&ev).expect("parse");
        assert_eq!(parsed, payload);
        assert_eq!(presence_author(&ev), keys.public_key());
        assert!(!ev.as_json().contains("working-state")); // content is payload json
    }

    #[test]
    fn test_non_presence_parses_none() {
        let keys = Keys::generate();
        let note = build_event(
            &keys,
            nostr_sdk::nostr::Kind::TextNote,
            "hi",
            Vec::<Tag>::new(),
            Timestamp::from(1_700_000_000_u64),
        )
        .expect("build");
        assert!(parse_presence(&note).is_none());
    }

    #[test]
    fn test_presence_event_verifies() {
        let keys = Keys::generate();
        let payload = PresencePayload {
            state: PresenceState::Done,
            status: None,
            ts: 1_700_000_000,
        };
        let ev = build_presence_event(&keys, &payload, Timestamp::from(1_700_000_000_u64))
            .expect("build");
        assert!(crate::nostr::event::verify_event(&ev).is_ok());
    }
}
