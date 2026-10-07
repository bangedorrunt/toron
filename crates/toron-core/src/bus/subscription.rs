// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Subscription model for the [`Bus`](crate::bus::Bus) trait.
//!
//! A subscription delivers matched events then an EOSE-equivalent marker,
//! followed by live events. Transport-agnostic: the bus implementation feeds a
//! channel; `RelayBus` maps WS frames onto the same shape so mailbox code is
//! identical for both buses.

use tokio::sync::mpsc;

use crate::nostr::Event;

/// Opaque subscription identifier.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SubscriptionId(pub String);

impl SubscriptionId {
    /// New subscription id from any stringable value.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A single item delivered to a subscription.
#[derive(Debug, Clone)]
pub enum SubscriptionEvent {
    /// A matched event (historical replay or live).
    Event(Event),
    /// EOSE-equivalent: end of stored-history replay; live events follow.
    Eose,
    /// Server closed the subscription (e.g. CLOSED frame on a relay).
    Closed(String),
}

/// Releases a subscription's bus-side registration when its owner drops.
///
/// The registry outlives every individual subscription, so cleanup has to
/// ride the subscription's own scope. Relying on a later publish to notice a
/// closed channel leaks the registration for the life of the process: an
/// access-gated subscriber is never fanned to, so nothing ever observes the
/// close (bd-cqo2).
pub(crate) type ReleaseOnDrop = Box<dyn FnOnce() + Send + 'static>;

/// A live subscription to the bus.
///
/// Consume with [`next()`](Self::next). There is no `Stream` conversion: this
/// type has no `Stream` impl, and no accessor hands out the raw
/// `mpsc::Receiver`, so `ReceiverStream::new(...)` cannot be built from it. An
/// earlier version of this doc claimed otherwise and pointed at
/// `ReceiverStream::new(sub.into_inner())`, which has not compiled since
/// `into_inner` started returning [`DetachedSubscription`].
///
/// [`into_inner`](Self::into_inner) hands the live stream to another owner
/// while carrying the bus registration with it, so detaching cannot
/// resurrect the leak this type's `Drop` exists to prevent.
pub struct Subscription {
    id: SubscriptionId,
    /// `None` once the receiver has been detached by [`into_inner`].
    rx: Option<mpsc::Receiver<SubscriptionEvent>>,
    /// Unregisters this id from the bus when dropped. `None` for transports
    /// that hold no registry (relay, wake sidecar).
    release: Option<ReleaseOnDrop>,
}

impl Subscription {
    /// Create a subscription from a channel receiver.
    pub fn new(id: SubscriptionId, rx: mpsc::Receiver<SubscriptionEvent>) -> Self {
        Self {
            id,
            rx: Some(rx),
            release: None,
        }
    }

    /// Attach the bus-side unregistration to run on drop.
    pub(crate) fn with_release(mut self, release: ReleaseOnDrop) -> Self {
        self.release = Some(release);
        self
    }

    /// The subscription id.
    pub const fn id(&self) -> &SubscriptionId {
        &self.id
    }

    /// Await the next event from the subscription.
    ///
    /// Returns `None` when the bus closed this subscription or dropped it.
    pub async fn next(&mut self) -> Option<SubscriptionEvent> {
        self.rx.as_mut()?.recv().await
    }

    /// Reject a subscription that has been closed by the server.
    ///
    /// Hands back the raw receiver *and* keeps the bus registration alive
    /// for as long as that receiver lives, so detaching never resurrects the
    /// leak this type exists to prevent.
    pub fn into_inner(mut self) -> DetachedSubscription {
        DetachedSubscription {
            rx: self.rx.take(),
            release: self.release.take(),
        }
    }

    /// Channel capacity used when the bus spawns a subscription.
    ///
    /// bd-bus-scan: raised 256 → 1024 — the mailbox's trusted projection
    /// subscriber can burst hundreds of bus events during an inbox catch-up
    /// (the Sep-1 "subscription channel full" stall); a full channel drops
    /// publisher-side with the event recoverable via store replay, so the
    /// larger buffer just defers drops while bounded (1024 × ~500 B ≈ 0.5 MB
    /// per subscriber).
    pub const DEFAULT_CAPACITY: usize = 1024;
}

/// A receiver detached from its `Subscription` via
/// [`Subscription::into_inner`].
///
/// Carries the bus registration with it so the unregistration still runs when
/// the last owner of the stream goes away.
pub struct DetachedSubscription {
    rx: Option<mpsc::Receiver<SubscriptionEvent>>,
    release: Option<ReleaseOnDrop>,
}

impl DetachedSubscription {
    /// Await the next event, `None` once the bus closed the subscription.
    pub async fn next(&mut self) -> Option<SubscriptionEvent> {
        self.rx.as_mut()?.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        // The scope owns cleanup, on every exit path: normal completion,
        // an early return, or cancellation. There is no later observer that
        // could notice the channel closed, so this is the only release point.
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

impl Drop for DetachedSubscription {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            release();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscription_delivers_events_and_eose() {
        let (tx, rx) = mpsc::channel(Subscription::DEFAULT_CAPACITY);
        let mut sub = Subscription::new(SubscriptionId::new("t"), rx);

        let keys = crate::nostr::Keys::generate();
        let ev = crate::nostr::event::build_event(
            &keys,
            crate::nostr::Kind::TextNote,
            "hello",
            Vec::<crate::nostr::Tag>::new(),
            crate::nostr::Timestamp::now(),
        )
        .expect("build");

        tx.send(SubscriptionEvent::Event(ev.clone())).await.unwrap();
        tx.send(SubscriptionEvent::Eose).await.unwrap();

        match sub.next().await {
            Some(SubscriptionEvent::Event(e)) => assert_eq!(e, ev),
            other => panic!("expected Event, got {other:?}"),
        }
        assert!(matches!(sub.next().await, Some(SubscriptionEvent::Eose)));
        // Channel still open but empty.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), sub.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_subscription_returns_none_when_dropped() {
        let (tx, rx) = mpsc::channel(Subscription::DEFAULT_CAPACITY);
        let mut sub = Subscription::new(SubscriptionId::new("t"), rx);
        drop(tx);
        assert!(sub.next().await.is_none());
    }

    #[tokio::test]
    async fn test_closed_event_surfaces() {
        let (tx, rx) = mpsc::channel(Subscription::DEFAULT_CAPACITY);
        let mut sub = Subscription::new(SubscriptionId::new("t"), rx);
        tx.send(SubscriptionEvent::Closed("rate limited".into()))
            .await
            .unwrap();
        assert!(matches!(
            sub.next().await,
            Some(SubscriptionEvent::Closed(_))
        ));
    }
}
