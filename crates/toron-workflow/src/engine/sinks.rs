// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! DI sink newtypes (F21.ENGINE-SPLIT move from the former single-file
//! engine): approval/spawn promise-id side-channels. Distinct newtypes (not
//! `type` aliases) so the Resonate dependency map keys each by a unique
//! `TypeId`.

use std::sync::Arc;

/// Optional side-channel for approval promise-ids (embedding/test hook). When
/// a run parks on `request_approval`, its promise_id is pushed here so the
/// embedding can publish the kind-46011 pending event.
///
/// Distinct newtype (not a `type` alias): the Resonate dependency map keys by
/// `TypeId` of the value type, so two `Arc<Mutex<Vec<String>>>` aliases would
/// collide and `get_dependency` would resolve both sinks to the LAST
/// registered — a real 0.6-migration regression (a1fe947). The newtype gives
/// each sink a unique `TypeId`; `Deref` keeps the `.lock().await` call sites
/// unchanged.
#[derive(Clone, Default)]
pub struct ApprovalSink(Arc<tokio::sync::Mutex<Vec<String>>>);

impl std::ops::Deref for ApprovalSink {
    type Target = tokio::sync::Mutex<Vec<String>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Side-channel for `spawn` promise-ids (ADR-0005 Spec 4a). When a run parks
/// on a spawn-await, its promise_id is pushed here so the embedding can emit
/// the kind-43001 job carrying it in the `spec`.
///
/// Distinct newtype — see [`ApprovalSink`] for why (TypeId collision in the
/// Resonate dependency map).
#[derive(Clone, Default)]
pub struct SpawnSink(Arc<tokio::sync::Mutex<Vec<String>>>);

impl std::ops::Deref for SpawnSink {
    type Target = tokio::sync::Mutex<Vec<String>>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
