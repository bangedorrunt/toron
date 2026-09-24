// governed-by: ADR-0003 Rev9 (GS-061 / Buzz #7620 delivery-cursor dedup)
#![forbid(unsafe_code)]

//! Session **delivery-cursor** dedup for prompt/thread context (Buzz #7620).
//!
//! Deduplicate by what this **session has already been delivered**, not by
//! "agent key signed ⇒ seen". Replaces agent-key heuristics in prompt assembly
//! / mention context. No chat-UX clone — effect-oriented honesty only.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// Opaque session id (ACP session analogue — toron dispatch/session key).
pub type SessionId = String;

/// Event/message id already delivered into a session's context.
pub type DeliveryId = String;

/// Per-session delivery cursor (ordered ids for newest-first windows).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeliveryCursor {
    /// Ids already delivered to this session (set for O(1) membership).
    pub delivered: BTreeSet<DeliveryId>,
    /// Monotonic cursor token (optional; host may map to composite cursor).
    pub cursor: u64,
}

/// Registry of session delivery cursors.
#[derive(Debug, Default)]
pub struct DeliveryCursorStore {
    inner: Mutex<HashMap<SessionId, DeliveryCursor>>,
}

/// Effect of a dedup decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryEffect {
    /// Id is new for this session — include in context and advance cursor.
    Deliver { session: SessionId, id: DeliveryId, cursor: u64 },
    /// Already delivered to this session — skip (not "agent key signed").
    SkipAlreadyDelivered { session: SessionId, id: DeliveryId },
}

impl DeliveryCursorStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter `candidate_ids` to those not yet delivered to `session`,
    /// advancing the cursor for each newly delivered id (order preserved).
    pub fn filter_undelivered(
        &self,
        session: &str,
        candidate_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> (Vec<DeliveryId>, Vec<DeliveryEffect>) {
        let mut g = self.inner.lock().expect("delivery cursor lock");
        let entry = g.entry(session.to_string()).or_default();
        let mut keep = Vec::new();
        let mut effects = Vec::new();
        for id in candidate_ids {
            let id = id.into();
            if entry.delivered.contains(&id) {
                effects.push(DeliveryEffect::SkipAlreadyDelivered {
                    session: session.into(),
                    id,
                });
            } else {
                entry.delivered.insert(id.clone());
                entry.cursor = entry.cursor.saturating_add(1);
                effects.push(DeliveryEffect::Deliver {
                    session: session.into(),
                    id: id.clone(),
                    cursor: entry.cursor,
                });
                keep.push(id);
            }
        }
        (keep, effects)
    }

    /// Snapshot cursor for a session.
    pub fn cursor(&self, session: &str) -> DeliveryCursor {
        self.inner
            .lock()
            .expect("delivery cursor lock")
            .get(session)
            .cloned()
            .unwrap_or_default()
    }
}

/// Pure helper: reject agent-key-as-seen heuristic — signing key ≠ delivery.
pub fn agent_key_is_not_delivery_proof(_agent_pubkey_hex: &str, _event_id: &str) -> bool {
    // Intentionally always true as a documentation assertion for callers:
    // delivery membership must come from DeliveryCursorStore, never from
    // "this agent key signed the event".
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_by_session_delivery_not_agent_key() {
        let store = DeliveryCursorStore::new();
        let (keep, fx) = store.filter_undelivered("sess-1", ["e1", "e2", "e1"]);
        assert_eq!(keep, vec!["e1", "e2"]);
        assert!(matches!(fx[2], DeliveryEffect::SkipAlreadyDelivered { .. }));
        // Same event, different session → deliver again (session-scoped).
        let (keep2, _) = store.filter_undelivered("sess-2", ["e1"]);
        assert_eq!(keep2, vec!["e1"]);
        assert!(agent_key_is_not_delivery_proof("npub1abc", "e1"));
    }

    #[test]
    fn cursor_advances_monotonically() {
        let store = DeliveryCursorStore::new();
        store.filter_undelivered("s", ["a", "b"]);
        assert_eq!(store.cursor("s").cursor, 2);
    }
}
