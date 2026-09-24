// governed-by: ADR-0003 Rev9 (GS-061 / Buzz #7620 delivery-cursor dedup)
#![forbid(unsafe_code)]

//! Session **delivery-cursor** dedup for prompt/thread context (Buzz #7620).
//!
//! Deduplicate by what this **session has already been delivered**, not by
//! "agent key signed ⇒ seen". Replaces agent-key heuristics in prompt assembly
//! / mention context. No chat-UX clone — effect-oriented honesty only.
//!
//! Hosts must **peek** undelivered ids, then build/publish, and only
//! [`DeliveryCursorStore::mark_delivered`] after success. Advancing before
//! publish marks a parent delivered on a failed publish → silent kind-43001 loss.

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
    /// Id newly marked delivered for this session — cursor advanced.
    Deliver {
        session: SessionId,
        id: DeliveryId,
        cursor: u64,
    },
    /// Already delivered to this session — skip (not "agent key signed").
    SkipAlreadyDelivered { session: SessionId, id: DeliveryId },
}

impl DeliveryCursorStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Report which `candidate_ids` are not yet delivered to `session`.
    /// Does **not** advance the cursor — hosts must [`Self::mark_delivered`]
    /// only after a successful build/publish.
    ///
    /// Intra-batch duplicates are reported as [`DeliveryEffect::SkipAlreadyDelivered`]
    /// (same call treats the first sighting as the sole undelivered keep).
    pub fn peek_undelivered(
        &self,
        session: &str,
        candidate_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> (Vec<DeliveryId>, Vec<DeliveryEffect>) {
        let mut g = self.inner.lock().expect("delivery cursor lock");
        let entry = g.entry(session.to_string()).or_default();
        let mut keep = Vec::new();
        let mut seen_batch = BTreeSet::new();
        let mut effects = Vec::new();
        for id in candidate_ids {
            let id = id.into();
            if entry.delivered.contains(&id) || seen_batch.contains(&id) {
                effects.push(DeliveryEffect::SkipAlreadyDelivered {
                    session: session.into(),
                    id,
                });
            } else {
                seen_batch.insert(id.clone());
                keep.push(id);
            }
        }
        (keep, effects)
    }

    /// Alias for [`Self::peek_undelivered`] — does **not** advance the cursor.
    ///
    /// Historical name retained; prior advance-on-filter semantics were a
    /// silent-loss hazard (mark before publish). Prefer `peek_undelivered` +
    /// `mark_delivered` at call sites.
    pub fn filter_undelivered(
        &self,
        session: &str,
        candidate_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> (Vec<DeliveryId>, Vec<DeliveryEffect>) {
        self.peek_undelivered(session, candidate_ids)
    }

    /// Advance the session cursor for successfully published/delivered ids.
    /// Idempotent: already-delivered ids emit [`DeliveryEffect::SkipAlreadyDelivered`].
    pub fn mark_delivered(
        &self,
        session: &str,
        ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> Vec<DeliveryEffect> {
        let mut g = self.inner.lock().expect("delivery cursor lock");
        let entry = g.entry(session.to_string()).or_default();
        let mut effects = Vec::new();
        for id in ids {
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
                    id,
                    cursor: entry.cursor,
                });
            }
        }
        effects
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
        let (keep, fx) = store.peek_undelivered("sess-1", ["e1", "e2", "e1"]);
        assert_eq!(keep, vec!["e1", "e2"]);
        assert!(matches!(fx[0], DeliveryEffect::SkipAlreadyDelivered { .. }));
        // Peek must not advance — mark after "publish".
        store.mark_delivered("sess-1", ["e1", "e2"]);
        // Same event, different session → deliver again (session-scoped).
        let (keep2, _) = store.peek_undelivered("sess-2", ["e1"]);
        assert_eq!(keep2, vec!["e1"]);
        store.mark_delivered("sess-2", ["e1"]);
        assert!(agent_key_is_not_delivery_proof("npub1abc", "e1"));
    }

    #[test]
    fn cursor_advances_monotonically_on_mark() {
        let store = DeliveryCursorStore::new();
        let (keep, _) = store.peek_undelivered("s", ["a", "b"]);
        assert_eq!(keep, vec!["a", "b"]);
        assert_eq!(store.cursor("s").cursor, 0, "peek must not advance");
        store.mark_delivered("s", ["a", "b"]);
        assert_eq!(store.cursor("s").cursor, 2);
    }

    /// Regression: peek keeps cursor; failed publish path does not mark;
    /// successful path marks; second attempt skips.
    #[test]
    fn peek_keeps_cursor_failed_publish_does_not_mark_success_marks() {
        let store = DeliveryCursorStore::new();
        let session = "sess-reg";
        let id = "parent-43001";

        let (keep, _) = store.peek_undelivered(session, [id]);
        assert_eq!(keep, vec![id]);
        assert_eq!(store.cursor(session).cursor, 0);
        assert!(store.cursor(session).delivered.is_empty());

        // Failed publish: do not mark — id remains undelivered.
        let (keep_retry, _) = store.peek_undelivered(session, [id]);
        assert_eq!(
            keep_retry,
            vec![id],
            "failed publish must leave id undelivered for retry"
        );

        // Successful path marks.
        let fx = store.mark_delivered(session, [id]);
        assert!(matches!(fx[0], DeliveryEffect::Deliver { cursor: 1, .. }));
        assert_eq!(store.cursor(session).cursor, 1);
        assert!(store.cursor(session).delivered.contains(id));

        // Second attempt skips.
        let (keep2, fx2) = store.peek_undelivered(session, [id]);
        assert!(keep2.is_empty());
        assert!(matches!(
            fx2[0],
            DeliveryEffect::SkipAlreadyDelivered { .. }
        ));
    }
}
