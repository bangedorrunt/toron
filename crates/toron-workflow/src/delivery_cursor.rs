// governed-by: ADR-0003 Rev9 (GS-061 / Buzz #7620 delivery-cursor dedup)
#![forbid(unsafe_code)]

//! Session **delivery-cursor** dedup for prompt/thread context (Buzz #7620).
//!
//! Deduplicate by what this **session has already been delivered**, not by
//! "agent key signed ⇒ seen". Replaces agent-key heuristics in prompt assembly
//! / mention context. No chat-UX clone — effect-oriented honesty only.
//!
//! Hosts must **peek** (or **reserve**) undelivered ids, then build/publish,
//! and only [`DeliveryCursorStore::mark_delivered`] after success. Advancing
//! before publish marks a parent delivered on a failed publish → silent
//! kind-43001 loss.
//!
//! Concurrent publish paths must [`Self::reserve_undelivered`] under the lock
//! before awaiting transport: a plain peek→await→mark race lets two callers
//! both keep the same id and double-publish. Release the reservation on
//! transport failure so the id stays retryable.

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
    /// Ids reserved by an in-flight publish (not yet committed delivered).
    #[serde(default)]
    pub in_flight: BTreeSet<DeliveryId>,
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
    /// Id is reserved by an in-flight publish for this session.
    SkipInFlight { session: SessionId, id: DeliveryId },
}

impl DeliveryCursorStore {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Report which `candidate_ids` are not yet delivered (and not in-flight)
    /// to `session`. Does **not** advance the cursor or reserve — hosts that
    /// publish concurrently must use [`Self::reserve_undelivered`] instead.
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
            } else if entry.in_flight.contains(&id) {
                effects.push(DeliveryEffect::SkipInFlight {
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
    /// `mark_delivered` at call sites (or `reserve_undelivered` before await).
    pub fn filter_undelivered(
        &self,
        session: &str,
        candidate_ids: impl IntoIterator<Item = impl Into<String>>,
    ) -> (Vec<DeliveryId>, Vec<DeliveryEffect>) {
        self.peek_undelivered(session, candidate_ids)
    }

    /// Like [`Self::peek_undelivered`], but reserves kept ids as in-flight under
    /// the same lock so a concurrent reserve for the same session/parent cannot
    /// also keep them. Caller must [`Self::mark_delivered`] after successful
    /// publish, or [`Self::release_reservation`] on transport failure.
    pub fn reserve_undelivered(
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
            } else if entry.in_flight.contains(&id) {
                effects.push(DeliveryEffect::SkipInFlight {
                    session: session.into(),
                    id,
                });
            } else {
                seen_batch.insert(id.clone());
                entry.in_flight.insert(id.clone());
                keep.push(id);
            }
        }
        (keep, effects)
    }

    /// Drop in-flight reservations without marking delivered (transport failure).
    /// Ids become peekable/reservable again. Idempotent for unknown ids.
    pub fn release_reservation(
        &self,
        session: &str,
        ids: impl IntoIterator<Item = impl Into<String>>,
    ) {
        let mut g = self.inner.lock().expect("delivery cursor lock");
        let entry = g.entry(session.to_string()).or_default();
        for id in ids {
            entry.in_flight.remove(id.into().as_str());
        }
    }

    /// Advance the session cursor for successfully published/delivered ids.
    /// Clears any in-flight reservation for those ids.
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
            entry.in_flight.remove(&id);
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


/// RAII guard for [`DeliveryCursorStore::reserve_undelivered`] reservations
/// (GS-062 INFLIGHT-RESERVATION-LEAK).
///
/// On Drop (cancel/panic/early return) releases in-flight ids so they are
/// retryable. Call [`Self::commit`] after successful publish to mark delivered
/// and disarm the release.
pub struct ReservationGuard<'a> {
    store: &'a DeliveryCursorStore,
    session: SessionId,
    ids: Vec<DeliveryId>,
    armed: bool,
}

impl<'a> ReservationGuard<'a> {
    /// Take ownership of `ids` already reserved under `session`.
    pub fn new(
        store: &'a DeliveryCursorStore,
        session: impl Into<String>,
        ids: Vec<DeliveryId>,
    ) -> Self {
        Self {
            store,
            session: session.into(),
            ids,
            armed: true,
        }
    }

    /// Reserved ids (read-only).
    pub fn ids(&self) -> &[DeliveryId] {
        &self.ids
    }

    /// Mark reserved ids delivered and disarm (Drop will not release).
    pub fn commit(mut self) {
        if self.armed {
            self.store
                .mark_delivered(&self.session, self.ids.iter().cloned());
            self.armed = false;
        }
    }
}

impl Drop for ReservationGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.store
                .release_reservation(&self.session, self.ids.iter().cloned());
            self.armed = false;
        }
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
    use std::sync::Arc;
    use std::thread;

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

    /// Multi-parent batch: peek/mark ALL parents; skip only when none remain.
    #[test]
    fn multi_parent_peek_mark_skips_only_when_none_remain() {
        let store = DeliveryCursorStore::new();
        let session = "agent-a";
        let parents = ["p1", "p2", "p3"];

        let (keep, _) = store.peek_undelivered(session, parents);
        assert_eq!(keep, vec!["p1", "p2", "p3"]);

        // First parent already delivered — siblings still undelivered.
        store.mark_delivered(session, ["p1"]);
        let (keep2, _) = store.peek_undelivered(session, parents);
        assert_eq!(
            keep2,
            vec!["p2", "p3"],
            "later parents must remain after first is delivered"
        );
        assert!(!keep2.is_empty(), "must not skip whole batch");

        store.mark_delivered(session, keep2);
        let (keep3, _) = store.peek_undelivered(session, parents);
        assert!(
            keep3.is_empty(),
            "skip whole batch only when none remain undelivered"
        );
    }

    /// Concurrent reserve: only one caller keeps the id; the other sees in-flight.
    #[test]
    fn reserve_prevents_concurrent_double_keep() {
        let store = Arc::new(DeliveryCursorStore::new());
        let session = "sess-race";
        let id = "parent-race";

        let (keep_a, _) = store.reserve_undelivered(session, [id]);
        assert_eq!(keep_a, vec![id]);
        assert!(store.cursor(session).in_flight.contains(id));

        let (keep_b, fx_b) = store.reserve_undelivered(session, [id]);
        assert!(keep_b.is_empty(), "second reserve must not keep in-flight id");
        assert!(matches!(fx_b[0], DeliveryEffect::SkipInFlight { .. }));

        // Transport failure releases — id becomes reservable again.
        store.release_reservation(session, [id]);
        assert!(!store.cursor(session).in_flight.contains(id));
        let (keep_c, _) = store.reserve_undelivered(session, [id]);
        assert_eq!(keep_c, vec![id]);

        store.mark_delivered(session, [id]);
        assert!(!store.cursor(session).in_flight.contains(id));
        assert!(store.cursor(session).delivered.contains(id));
        let (keep_d, _) = store.reserve_undelivered(session, [id]);
        assert!(keep_d.is_empty());
    }

    /// Threads racing reserve: exactly one winner keeps the id.
    #[test]
    fn concurrent_reserve_single_winner() {
        let store = Arc::new(DeliveryCursorStore::new());
        let session = "sess-threads";
        let id = "parent-threads";
        let winners = Arc::new(Mutex::new(0u32));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let store = store.clone();
            let winners = winners.clone();
            handles.push(thread::spawn(move || {
                let (keep, _) = store.reserve_undelivered(session, [id]);
                if !keep.is_empty() {
                    *winners.lock().expect("winners") += 1;
                    // Simulate publish then mark.
                    store.mark_delivered(session, keep);
                }
            }));
        }
        for h in handles {
            h.join().expect("join");
        }
        assert_eq!(*winners.lock().expect("winners"), 1);
        assert!(store.cursor(session).delivered.contains(id));
    }

    /// GS-062 INFLIGHT-RESERVATION-LEAK: Drop clears in_flight without mark.
    #[test]
    fn reservation_guard_drop_releases_inflight() {
        let store = DeliveryCursorStore::new();
        let session = "sess-guard";
        let id = "parent-guard";
        let (keep, _) = store.reserve_undelivered(session, [id]);
        assert_eq!(keep, vec![id]);
        {
            let _g = ReservationGuard::new(&store, session, keep);
            assert!(store.cursor(session).in_flight.contains(id));
        }
        assert!(
            !store.cursor(session).in_flight.contains(id),
            "Drop must clear in_flight"
        );
        assert!(!store.cursor(session).delivered.contains(id));
        // Reservable again after Drop.
        let (keep2, _) = store.reserve_undelivered(session, [id]);
        assert_eq!(keep2, vec![id]);
        let g = ReservationGuard::new(&store, session, keep2);
        g.commit();
        assert!(store.cursor(session).delivered.contains(id));
        assert!(store.cursor(session).in_flight.is_empty());
    }

    /// GS-062 INFLIGHT-RESERVATION-LEAK: unwind/panic path clears via Drop.
    #[test]
    fn reservation_guard_drop_on_panic_unwind() {
        let store = Arc::new(DeliveryCursorStore::new());
        let session = "sess-panic";
        let id = "parent-panic";
        let (keep, _) = store.reserve_undelivered(session, [id]);
        let store2 = store.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = ReservationGuard::new(store2.as_ref(), session, keep);
            panic!("simulated cancel between reserve and mark");
        }));
        assert!(result.is_err());
        assert!(
            !store.cursor(session).in_flight.contains(id),
            "panic Drop must clear in_flight"
        );
    }
}
