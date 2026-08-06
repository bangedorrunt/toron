// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Bus trait + Filter/Subscription model.
//!
//! Mailbox core and projection depend only on [`Bus`] — swap `InProcessBus`
//! (channels) vs `RelayBus` (WS) without tool changes. Events are
//! wire-identical across both.

pub mod event_store;
pub mod filter;
pub mod in_process;
#[cfg(test)]
mod mock_relay;
pub mod relay_bus;
pub mod scoping;
pub mod subscription;

pub use event_store::BusEventStore;
pub use filter::{project_filter, Filter, LabelValue};
pub use in_process::InProcessBus;
pub use relay_bus::{RelayBus, RelayBusConfig, RelayConfig, RelayRole};
pub use scoping::{is_metadata_hiding_kind, project_scoping_tags, with_project_scoping};
pub use subscription::{Subscription, SubscriptionEvent, SubscriptionId};

use nostr_sdk::nostr::Event;

/// Outcome of a successful publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOk {
    /// Wire-identical event id as accepted by the bus.
    pub event_id: nostr_sdk::nostr::EventId,
    /// True when the event was newly stored; false when deduped (duplicate).
    pub stored: bool,
}

/// Bus health snapshot (liveness for the `health_check` tool).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Health {
    /// Bus is accepting publishes and serving subscriptions.
    pub ok: bool,
    /// Human-readable detail.
    pub message: String,
}

/// Error returned by the bus.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    #[error("event failed verification: {0}")]
    InvalidEvent(String),
    #[error("subscription rejected: {0}")]
    Subscription(String),
    #[error("publish rejected: {0}")]
    Publish(String),
    #[error("bus is shutting down")]
    Shutdown,
    #[error("io/transport failure: {0}")]
    Transport(String),
}

/// A bus is a pub/sub transport for wire-identical Nostr events.
///
/// Implementations run the same validation pipeline (verify → dedup →
/// fanout → search/audit/workflow hooks, per ADR-0003 Buzz adoption) and emit
/// events that a real relay would accept unchanged.
#[async_trait::async_trait]
pub trait Bus: Send + Sync + 'static {
    /// Publish a signed event. Deduplicates on `(kind, pubkey, id)`.
    async fn publish(&self, event: Event) -> Result<PublishOk, BusError>;

    /// Open a subscription for events matching `filter`.
    ///
    /// The returned subscription replays stored history matching the filter,
    /// emits an EOSE-equivalent marker, then streams live events.
    async fn subscribe(&self, filter: Filter) -> Result<Subscription, BusError>;

    /// Liveness probe.
    async fn health(&self) -> Result<Health, BusError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use nostr_sdk::nostr::JsonUtil;

    #[test]
    fn test_publish_ok_shapes() {
        let id = nostr_sdk::nostr::EventId::from_hex(
            "b32828720d7e8a80dd5165a6cd31b2c70e8d25b83c387209cb138e77d7e7e05b",
        )
        .expect("id");
        assert_eq!(
            PublishOk {
                event_id: id,
                stored: true,
            },
            PublishOk {
                event_id: id,
                stored: true,
            }
        );
        assert_ne!(
            PublishOk {
                event_id: id,
                stored: true,
            },
            PublishOk {
                event_id: id,
                stored: false,
            }
        );
    }

    #[test]
    fn test_health_shapes() {
        let h = Health {
            ok: true,
            message: "ok".into(),
        };
        assert!(h.ok);
        assert!(
            !Health {
                ok: false,
                message: "down".into(),
            }
            .ok
        );
    }

    #[test]
    fn test_bus_error_display_redacts_nothing_sensitive() {
        let e = BusError::InvalidEvent("id mismatch".into());
        assert_eq!(format!("{e}"), "event failed verification: id mismatch");
    }

    // Compile-time: the trait object is usable as a dependency for mailbox.
    fn _assert_trait_object(_b: &dyn Bus) {}

    #[test]
    fn test_event_roundtrips_through_json() {
        use crate::nostr::event::build_event;
        use crate::nostr::kinds::PROJECT_MANIFEST;
        use nostr_sdk::nostr::{Tag, Timestamp};
        let keys = nostr_sdk::nostr::Keys::generate();
        let ev = build_event(
            &keys,
            crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
            "manifest",
            vec![Tag::parse(["d", "demo"]).expect("tag")],
            Timestamp::now(),
        )
        .expect("build");
        let json = ev.as_json();
        let parsed = nostr_sdk::nostr::Event::from_json(json).expect("parse");
        assert_eq!(parsed, ev);
        assert_eq!(parsed.kind.as_u16(), PROJECT_MANIFEST);
    }
}
