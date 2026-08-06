// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Subscription model for the [`Bus`](crate::bus::Bus) trait.
//!
//! A subscription delivers matched events then an EOSE-equivalent marker,
//! followed by live events. Transport-agnostic: the bus implementation feeds a
//! channel; `RelayBus` maps WS frames onto the same shape so mailbox code is
//! identical for both buses.

use tokio::sync::mpsc;

use nostr_sdk::nostr::Event;

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

/// A live subscription to the bus.
///
/// Consume with [`next()`](Self::next) or `Stream::next` via
/// [`tokio_stream`]'s `ReceiverStream` wrapper.
pub struct Subscription {
    id: SubscriptionId,
    rx: mpsc::Receiver<SubscriptionEvent>,
}

impl Subscription {
    /// Create a subscription from a channel receiver.
    pub fn new(id: SubscriptionId, rx: mpsc::Receiver<SubscriptionEvent>) -> Self {
        Self { id, rx }
    }

    /// The subscription id.
    pub fn id(&self) -> &SubscriptionId {
        &self.id
    }

    /// Await the next event from the subscription.
    ///
    /// Returns `None` when the bus closed this subscription or dropped it.
    pub async fn next(&mut self) -> Option<SubscriptionEvent> {
        self.rx.recv().await
    }

    /// Reject a subscription that has been closed by the server.
    pub fn into_inner(self) -> mpsc::Receiver<SubscriptionEvent> {
        self.rx
    }

    /// Channel capacity used when the bus spawns a subscription.
    pub const DEFAULT_CAPACITY: usize = 256;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_id_display() {
        let id = SubscriptionId::new("sub-1");
        assert_eq!(id.to_string(), "sub-1");
        assert_eq!(id, SubscriptionId::new("sub-1"));
    }

    #[tokio::test]
    async fn test_subscription_delivers_events_and_eose() {
        let (tx, rx) = mpsc::channel(Subscription::DEFAULT_CAPACITY);
        let mut sub = Subscription::new(SubscriptionId::new("t"), rx);

        let keys = nostr_sdk::nostr::Keys::generate();
        let ev = crate::nostr::event::build_event(
            &keys,
            nostr_sdk::nostr::Kind::TextNote,
            "hello",
            Vec::<nostr_sdk::nostr::Tag>::new(),
            nostr_sdk::nostr::Timestamp::now(),
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
