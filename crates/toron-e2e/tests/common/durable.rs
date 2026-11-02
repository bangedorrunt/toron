// governed-by: ADR-0043
#![forbid(unsafe_code)]

//! Durable-drive fixtures for the e2e port (bd-i84w): temp sqlite pools,
//! channel-bridged engine handles, and leaf seams over an injected async
//! runner — the same trio `toron-workflow/tests/durable_host.rs` uses, so
//! these scenarios prove the production path with no server process.
//!
//! Each scenario owns its drive loop (open → effects → verbs); this module
//! only builds the pieces. Sync driver verbs must run in `spawn_blocking`,
//! never on an async worker.

use std::sync::Arc;

use toron_core::index::durable_engine::ToronEngineStore;
use toron_core::index::durable_workflow::SyncWorkflowState;
use toron_core::journal::{JournalDb, ToronJournal};
use toron_workflow::engine::leaf::LeafSeams;
use toron_workflow::engine::linear_step::LinearStep;
use toron_workflow::engine::{AsyncEffectRunner, EffectJournalMode, VerifySink};
use toron_workflow::schema::Workflow;

/// Host time, unix millis.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

/// Temp sqlite pool with the toron schema migrated (engine + journal +
/// model tables). Returns the pool and the db path (caller removes it).
pub async fn test_pool(tag: &str) -> (sqlx::SqlitePool, std::path::PathBuf) {
    let db = std::env::temp_dir().join(format!(
        "toron-e2e-durable-{tag}-{}-{}.sqlite",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let pool = toron_core::index::schema::connect(db.to_str().unwrap())
        .await
        .expect("connect");
    toron_core::index::schema::migrate(&pool)
        .await
        .expect("migrate");
    (pool, db)
}

/// Channel-bridged engine handles over `pool`. Call from async context
/// (needs the runtime handle); use the handles only in `spawn_blocking`.
pub fn handles(pool: &sqlx::SqlitePool) -> (ToronEngineStore, ToronJournal, SyncWorkflowState) {
    let handle = tokio::runtime::Handle::current();
    (
        ToronEngineStore::spawn(pool.clone(), &handle),
        ToronJournal::spawn(JournalDb::new(pool.clone(), "e2e", 64), &handle),
        SyncWorkflowState::spawn(pool.clone(), &handle),
    )
}

/// Leaf seams over one async runner (Enforced: an effect with no journal
/// seam refuses instead of running unjournaled).
pub fn seams(runner: AsyncEffectRunner) -> LeafSeams {
    LeafSeams {
        async_runner: Some(runner),
        sync_runner: None,
        verify_sink: VerifySink::default(),
        journal_mode: EffectJournalMode::Enforced,
    }
}

/// Canonical workflow digest for run-scoped effect keys (same helper the
/// old engine path fed every invocation).
pub fn digest(workflow: &Workflow) -> String {
    toron_workflow::identity::workflow_digest(workflow)
}

/// Null-returning async runner (steps succeed; assertions read outputs).
pub fn null_runner() -> AsyncEffectRunner {
    Arc::new(|_: &toron_workflow::engine::ActionInvocation| {
        Box::pin(async move { Ok(serde_json::Value::Null) })
            as std::pin::Pin<
                Box<dyn std::future::Future<Output = anyhow::Result<serde_json::Value>> + Send>,
            >
    })
}

/// One linear step through the leaf boundary (guard → strict render →
/// single dispatch via `run_linear_step`).
pub async fn run_step(
    run_id: &str,
    workflow_name: &str,
    workflow_digest: &str,
    step: &toron_workflow::schema::Step,
    index: usize,
    trigger: &serde_json::Value,
    outputs: &mut serde_json::Map<String, serde_json::Value>,
    seams: &LeafSeams,
) -> serde_json::Value {
    toron_workflow::engine::linear_step::run_linear_step(
        LinearStep {
            run_id,
            workflow: workflow_name,
            workflow_digest,
            step,
            index,
            trigger,
            outputs,
        },
        seams,
    )
    .await
}
