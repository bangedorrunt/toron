// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Mailbox core — the 38-tool AM-Rust parity surface mapped onto the Nostr
//! bus (ADR-0003 Decision 1A + the Business Logic parity contract).
//!
//! This is the **mailbox layer**: tools publish signed events to the [`Bus`]
//! and query the projection. It depends only on the bus + nostr/keys/archive
//! layers — swap `InProcessBus` ↔ `RelayBus` without tool changes.
//!
//! bd-led.5.1 ships the Infrastructure cluster (health_check, ensure_project,
//! install/uninstall_precommit_guard). Subsequent `.5.x` beads add the other
//! clusters in-place.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nostr_sdk::nostr::{Event, Keys};

use crate::bus::{Bus, BusError, Filter, SubscriptionEvent};

pub mod build_slots;
pub mod contacts;
pub mod guard;
pub mod identity;
pub mod infra;
pub mod invariants;
pub mod macros;
pub mod matrix;
pub mod messaging;
pub mod product;
pub mod reservations;
pub mod search;
pub mod unwrap;

pub use build_slots::AcquireBuildSlotResult;
pub use contacts::{BusPolicySource, ContactLink, LinkStatus, RequestContact, RespondContact};
pub use guard::{
    guard_blocked_paths, install_precommit_guard, uninstall_precommit_guard,
    Reservation as GuardReservation,
};
pub use identity::{ListAgentsOptions, RegisteredAgent, WhoIs};
pub use infra::{HealthComponent, HealthReport, ProjectState};
pub use macros::{
    ContactHandshakeResult, PrepareThreadResult, ReservationCycleResult, StartSessionResult,
};
pub use messaging::{
    Delivery, DeliveryPayload, FetchInbox, InboxMessage, ReceiptResult, RecipientRole, SendMessage,
    SendResult,
};
pub use product::ProductInfo;
pub use reservations::{
    overlaps, AcquireResult, ConflictReport, GrantedReservation, PathConflict, ReleaseResult,
    RenewResult, Reservation,
};
pub use search::{SearchMessage, SearchMessages, ThreadSummary};
pub use unwrap::{
    order_by_rumor_time, AckCache, AutoPolicy, ContactPolicy, DedupKey, FixedPolicy,
    GitProjectionSink, PolicySource, ProjectOutcome, ProjectionSink, SqliteProjectionSink,
    UnwrapPipeline, UnwrappedMail,
};

/// Mailbox context: the bus every tool publishes to + the on-disk project
/// root layout. The operator key signs privileged events (kind-30010 project
/// manifests, revocations); `None` means no operator configured (reads only).
#[derive(Clone)]
pub struct Mailbox {
    bus: Arc<dyn Bus>,
    data_dir: PathBuf,
    operator: Option<Keys>,
    agent_passphrase: Option<String>,
    receipt_cache: Arc<parking_lot::Mutex<HashMap<String, i64>>>,
    index_pool: Option<sqlx::SqlitePool>,
}

impl std::fmt::Debug for Mailbox {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Mailbox")
            .field("data_dir", &self.data_dir)
            .field("operator", &self.operator.is_some())
            .field("has_agent_passphrase", &self.agent_passphrase.is_some())
            .field("has_index_pool", &self.index_pool.is_some())
            .finish()
    }
}

impl Mailbox {
    /// Construct a mailbox over a bus + a data directory holding project
    /// roots (`{data_dir}/{slug}`).
    pub fn new(bus: Arc<dyn Bus>, data_dir: PathBuf, operator: Option<Keys>) -> Self {
        Self {
            bus,
            data_dir,
            operator,
            agent_passphrase: None,
            receipt_cache: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            index_pool: None,
        }
    }

    /// Set the agent-key passphrase (G1: unlocks ncryptsec at process start).
    pub fn with_agent_passphrase(mut self, passphrase: impl Into<String>) -> Self {
        self.agent_passphrase = Some(passphrase.into());
        self
    }

    /// Attach the SQLite index pool (contacts/agent links etc.). Without it,
    /// index-backed tools return a "no index pool" error.
    pub fn with_index_pool(mut self, pool: sqlx::SqlitePool) -> Self {
        self.index_pool = Some(pool);
        self
    }

    /// The SQLite index pool, if attached.
    pub fn index_pool(&self) -> Option<&sqlx::SqlitePool> {
        self.index_pool.as_ref()
    }

    /// The bus backing this mailbox.
    pub fn bus(&self) -> &Arc<dyn Bus> {
        &self.bus
    }

    /// The on-disk directory under which project roots live.
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// The operator key (if configured).
    pub fn operator(&self) -> Option<&Keys> {
        self.operator.as_ref()
    }
}

/// Drain a bus subscription to EOSE, returning all replayed events.
///
/// A subscription replays stored history matching `filter`, emits an
/// EOSE-equivalent marker, then streams live events. Callers that want a
/// consistent point-in-time snapshot read up to EOSE and ignore the live tail
/// (or keep the subscription for push delivery).
pub async fn collect_replay(bus: &dyn Bus, filter: Filter) -> Result<Vec<Event>, BusError> {
    let mut sub = bus.subscribe(filter).await?;
    let mut events = Vec::new();
    while let Some(SubscriptionEvent::Event(e)) = sub.next().await {
        events.push(e);
    }
    Ok(events)
}
