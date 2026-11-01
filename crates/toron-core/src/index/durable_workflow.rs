// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Persisted state for the pure linear durable workflow model.
//!
//! The model itself lives in `toron-workflow`; this store only keeps its JSON
//! by run id. The table is additive (`schema/additive/durable_workflow.sql`),
//! so an older binary that never reads it is correct and no store version is
//! raised. The store is plain async SQL; [`SyncWorkflowState`] gives a
//! synchronous host loop the same request/reply bridge as
//! [`crate::index::durable_engine::ToronEngineStore`].

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use sqlx::SqlitePool;

/// How long a bridge call waits for the store.
pub const STATE_BRIDGE_TIMEOUT: Duration = Duration::from_secs(30);

type Job = Box<dyn FnOnce(SqlitePool) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Durable run-model state by run id.
#[derive(Clone)]
pub struct WorkflowStateStore {
    pool: SqlitePool,
}

impl WorkflowStateStore {
    /// Store over the shared SQLite pool.
    pub const fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Load one run's state JSON, if present.
    ///
    /// # Errors
    ///
    /// Returns the SQL error when the store cannot be read.
    pub async fn load(&self, run_id: &str) -> anyhow::Result<Option<String>> {
        let row: Option<(String,)> =
            sqlx::query_as("SELECT state_json FROM durable_workflow_runs WHERE run_id = ?1")
                .bind(run_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(|(state,)| state))
    }

    /// Insert or replace one run's state JSON.
    ///
    /// # Errors
    ///
    /// Returns the SQL error when the store cannot be written.
    pub async fn save(&self, run_id: &str, state_json: &str) -> anyhow::Result<()> {
        let now = crate::index::workflow_effects::now_us();
        sqlx::query(
            "INSERT INTO durable_workflow_runs (run_id, state_json, updated_at) \
             VALUES (?1, ?2, ?3) \
             ON CONFLICT(run_id) DO UPDATE SET state_json = excluded.state_json, \
             updated_at = excluded.updated_at",
        )
        .bind(run_id)
        .bind(state_json)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Every persisted model, most recently updated first.
    ///
    /// The model is the only thing that knows whether a run is terminal, so the
    /// store returns all of them and the host parses. A restart sweep uses this
    /// to re-open the runs a previous process left parked or running.
    ///
    /// # Errors
    ///
    /// Returns the SQL error when the store cannot be read.
    pub async fn list(&self) -> anyhow::Result<Vec<(String, String)>> {
        let rows: Vec<(String, String)> = sqlx::query_as(
            "SELECT run_id, state_json FROM durable_workflow_runs ORDER BY updated_at DESC",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Delete one run's state (terminal cleanup).
    ///
    /// # Errors
    ///
    /// Returns the SQL error when the store cannot be written.
    pub async fn delete(&self, run_id: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM durable_workflow_runs WHERE run_id = ?1")
            .bind(run_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

/// Sync persistence seam for the pure run model. The engine is synchronous,
/// so the host owns the blocking; the async store sits behind a task.
pub trait RunState: Send + Sync {
    /// Load one run's state JSON, if present.
    ///
    /// # Errors
    ///
    /// Returns the store or bridge error when the state cannot be read.
    fn load(&self, run_id: &str) -> anyhow::Result<Option<String>>;

    /// Insert or replace one run's state JSON.
    ///
    /// # Errors
    ///
    /// Returns the store or bridge error when the state cannot be written.
    fn save(&self, run_id: &str, state_json: &str) -> anyhow::Result<()>;

    /// Delete one run's state (terminal cleanup).
    ///
    /// # Errors
    ///
    /// Returns the store or bridge error when the state cannot be deleted.
    fn delete(&self, run_id: &str) -> anyhow::Result<()>;

    /// Every persisted model as `(run_id, state_json)`, most recent first.
    ///
    /// Required, not defaulted: a default empty list would silently disable
    /// the restart sweep, and a parked run would never be re-opened.
    ///
    /// # Errors
    ///
    /// Returns the store or bridge error when the states cannot be read.
    fn list(&self) -> anyhow::Result<Vec<(String, String)>>;
}

/// Synchronous handle over `WorkflowStateStore`, same request/reply bridge as
/// `ToronEngineStore`. Never call from an async task; call from a blocking
/// fiber or a plain thread.
#[derive(Clone)]
pub struct SyncWorkflowState {
    tx: tokio::sync::mpsc::UnboundedSender<Job>,
}

impl SyncWorkflowState {
    /// Spawn the store task on `rt`.
    pub fn spawn(pool: SqlitePool, rt: &tokio::runtime::Handle) -> Self {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Job>();
        rt.spawn(async move {
            while let Some(job) = rx.recv().await {
                job(pool.clone()).await;
            }
        });
        Self { tx }
    }

    /// Run one store closure and wait for its result. Never call this straight
    /// from an async task; call it from a blocking fiber or a plain thread.
    fn call<T, F, Fut>(&self, run_id: &str, f: F) -> anyhow::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(SqlitePool) -> Fut + Send + 'static,
        Fut: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let (reply, rx) = std::sync::mpsc::channel::<anyhow::Result<T>>();
        let job: Job = Box::new(move |pool| {
            Box::pin(async move {
                let _ = reply.send(f(pool).await);
            })
        });
        self.tx
            .send(job)
            .map_err(|_| anyhow::anyhow!("workflow state store task is gone (run {run_id})"))?;
        rx.recv_timeout(STATE_BRIDGE_TIMEOUT).map_err(|e| {
            anyhow::anyhow!("workflow state store did not answer for run {run_id}: {e}")
        })?
    }
}

impl RunState for SyncWorkflowState {
    fn load(&self, run_id: &str) -> anyhow::Result<Option<String>> {
        let id = run_id.to_string();
        self.call(run_id, move |pool| async move {
            let store = WorkflowStateStore::new(pool);
            store.load(&id).await
        })
    }

    fn save(&self, run_id: &str, state_json: &str) -> anyhow::Result<()> {
        let id = run_id.to_string();
        let state = state_json.to_string();
        self.call(run_id, move |pool| async move {
            let store = WorkflowStateStore::new(pool);
            store.save(&id, &state).await
        })
    }

    fn delete(&self, run_id: &str) -> anyhow::Result<()> {
        let id = run_id.to_string();
        self.call(run_id, move |pool| async move {
            let store = WorkflowStateStore::new(pool);
            store.delete(&id).await
        })
    }

    fn list(&self) -> anyhow::Result<Vec<(String, String)>> {
        self.call("list", move |pool| async move {
            let store = WorkflowStateStore::new(pool);
            store.list().await
        })
    }
}
