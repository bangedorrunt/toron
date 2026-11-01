// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Persisted state for the pure linear durable workflow model.
//!
//! The model itself lives in `toron-workflow`; this store only keeps its JSON
//! by run id. The table is additive (`schema/additive/durable_workflow.sql`),
//! so an older binary that never reads it is correct and no store version is
//! raised. The sync model loop can use this through the same blocking-fiber
//! boundary as `ToronJournal`; the store is plain async SQL.

use sqlx::SqlitePool;

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
