// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Bus trait + Filter/Subscription model.
//!
//! Mailbox core and projection depend only on [`Bus`] — swap `InProcessBus`
//! (channels) vs `RelayBus` (WS) without tool changes. Events are
//! wire-identical across both.

pub mod addressable;
pub mod common;
pub mod convergence;
pub mod event_store;
pub mod federation;
pub mod filter;
pub mod http_guard;
pub mod in_process;
pub mod inspection;
#[cfg(test)]
mod mock_relay;
pub mod presence;
pub mod reconcile;
pub mod relay_bus;
pub mod scoping;
pub mod subscription;
pub mod ttl;
pub mod verify_tripwire;
pub mod wake;

pub use addressable::{
    AddressableKey, KeyPriority, is_addressable, is_revocation, nip01_replaceable_cmp,
    resolve_coordinate,
};
pub use convergence::{
    AuthorityOp, Convergence, ConvergenceEffect, ConvergenceMsg, presence_admit,
    tick as convergence_tick,
};
pub use event_store::BusEventStore;
pub use federation::{FederationIssue, FederationMode, FederationPolicy, PowDifficulty};
pub use filter::{
    Filter, LabelValue, authors_project_filter, kind_project_filter, project_filter,
    project_label_filter,
};
pub use http_guard::{FEED_MAX_LIMIT, clamp_feed_limit, is_private_ip};
pub use in_process::InProcessBus;
pub use relay_bus::{RelayBus, RelayBusConfig, RelayConfig, RelayRole};
pub use scoping::{
    is_ephemeral_kind, is_metadata_hiding_kind, project_scoping_tags, with_project_scoping,
};
pub use subscription::{Subscription, SubscriptionEvent, SubscriptionId};
pub use ttl::{DEFAULT_ENVELOPE_TTL_SECS, EnvelopeTtl};
pub use verify_tripwire::{
    PUBLISH_STAGE_ORDER, StageTrace, assert_stage_before, publish_source_verify_before_side_effects,
};

use crate::nostr::Event;

/// Outcome of a successful publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishOk {
    /// Wire-identical event id as accepted by the bus.
    pub event_id: crate::nostr::EventId,
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
    #[error("operation unsupported on this bus: {0}")]
    Unsupported(String),
    #[error("io/transport failure: {0}")]
    Transport(String),
}

/// A bus is a pub/sub transport for wire-identical Nostr events.
///
/// Implementations run the same validation pipeline (verify → dedup →
/// fanout → search/audit/workflow hooks, per ADR-0003 Buzz adoption) and emit
/// events that a real relay would accept unchanged.
///
/// `#[async_trait]` is DELIBERATELY retained here: `Bus` is used as
/// `Arc<dyn Bus>` (16 call sites) and native `async fn`-in-trait / RPITIT
/// traits are NOT dyn-compatible (measured on nightly 1.97: both forms give
/// E0038). Boxed futures (async-trait) are the correct tool for a dyn async
/// trait; `Backend` (never dyn-used) uses native AFIT instead.
#[async_trait::async_trait]
pub trait Bus: Send + Sync + 'static {
    /// Publish a signed event. Deduplicates on `(kind, pubkey, id)`.
    async fn publish(&self, event: Event) -> Result<PublishOk, BusError>;

    /// Open a subscription for events matching `filter`.
    ///
    /// The returned subscription replays stored history matching the filter,
    /// emits an EOSE-equivalent marker, then streams live events.
    async fn subscribe(&self, filter: Filter) -> Result<Subscription, BusError>;

    /// Open a TRUSTED subscription: identical to [`Self::subscribe`] but the
    /// A3 global-gate is bypassed — project-scoped events ARE delivered to a
    /// filter with no `#t`/`#l` scoping (P11.1 reactive ingress: the
    /// workflow-trigger host is the mailbox's own internal consumer and must
    /// see reservation/reaction events across all projects; a plain global
    /// `subscribe(Filter::new())` would be gated from every scoped event).
    ///
    /// Only embedders that hold the bus by construction (the mailbox host,
    /// the daemon) may use this — never a relay-facing subscription.
    async fn subscribe_trusted(&self, filter: Filter) -> Result<Subscription, BusError>;

    /// Liveness probe.
    async fn health(&self) -> Result<Health, BusError>;

    /// Operator-grade local removal (bd-channel-close-purge-bsqb): delete the
    /// stored events with these hex ids; returns rows removed. Only
    /// SQLite-backed buses implement it — the default refuses LOUDLY (a
    /// purge that silently no-ops is a silent drop wearing a success
    /// costume).
    async fn purge_ids(&self, ids: &[String]) -> Result<u64, BusError> {
        let _ = ids;
        Err(BusError::Unsupported(
            "this bus does not support local purge".to_string(),
        ))
    }
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
    use crate::nostr::{Tag, Timestamp};
    let keys = crate::nostr::Keys::generate();
    let ev = build_event(
        &keys,
        crate::nostr::kinds::toron_kind(PROJECT_MANIFEST),
        "manifest",
        vec![Tag::parse(["d", "demo"]).expect("tag")],
        Timestamp::now(),
    )
    .expect("build");
    let json = ev.as_json();
    let parsed = crate::nostr::Event::from_json(json).expect("parse");
    assert_eq!(parsed, ev);
    assert_eq!(parsed.kind.as_u16(), PROJECT_MANIFEST);
}
