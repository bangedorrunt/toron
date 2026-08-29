// governed-by: ADR-0024
// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! DI sink newtypes (F21.ENGINE-SPLIT move from the former single-file
//! engine): approval/spawn promise-id side-channels. Distinct newtypes (not
//! `type` aliases) so the Resonate dependency map keys each by a unique
//! `TypeId`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

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

/// Verify request passed to the embedding's `VerifySink` (ADR-0024 Spec 2).
#[derive(Debug, Clone)]
pub struct VerifyRequest {
    /// The shell command to verify (e.g. `cargo test -p toron-core ...`).
    pub command: String,
}

/// Verify result returned by the embedding (ADR-0024 Spec 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerifyResult {
    pub ok: bool,
    pub exit: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stdout_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stderr_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Embedding-provided VerifySink (ADR-0024 Spec 2). Unique TypeId, same pattern
/// as `ApprovalSink`/`SpawnSink`. `None` → `{ok:false, error:"verify sink missing"}`.
#[derive(Clone, Default)]
#[allow(clippy::type_complexity)]
pub struct VerifySink(
    pub  Arc<
        std::sync::Mutex<
            Option<Arc<dyn Fn(&VerifyRequest) -> anyhow::Result<VerifyResult> + Send + Sync>>,
        >,
    >,
);

impl std::fmt::Debug for VerifySink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VerifySink")
            .field("has_sink", &self.0.lock().unwrap().is_some())
            .finish()
    }
}
