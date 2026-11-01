// governed-by: ADR-0005
#![forbid(unsafe_code)]

//! Fallible [`EngineStorage`] over toron's SQLite store.
//!
//! The engine is synchronous and runs on a blocking fiber or OS thread, so
//! this handle talks to a task on a Tokio runtime through a request/reply
//! channel, exactly like [`crate::journal::ToronJournal`]. It never calls
//! `block_on` inside a runtime.
//!
//! The two tables are additive (`schema/additive/durable_engine.sql`): an
//! older binary that never reads them is correct, so this raises no store
//! version. `commit_run` and `commit_park` override the trait defaults with
//! one transaction each.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use sqlx::{Row, SqlitePool};

use crate::kernel::{EngineStorage, ParkedRow, RunPhase, RunRow, StorageError};

/// How long a bridge call waits for the store.
pub const ENGINE_BRIDGE_TIMEOUT: Duration = Duration::from_secs(30);

type Job = Box<dyn FnOnce(SqlitePool) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send>;

/// Synchronous [`EngineStorage`] handle over toron's SQLite pool.
#[derive(Clone)]
pub struct ToronEngineStore {
    tx: tokio::sync::mpsc::UnboundedSender<Job>,
}

impl ToronEngineStore {
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

    /// Run one store closure and wait for its typed result. Never call this
    /// straight from an async task; call it from a blocking fiber or a
    /// plain thread.
    fn call<T, F, Fut>(&self, f: F) -> Result<T, StorageError>
    where
        T: Send + 'static,
        F: FnOnce(SqlitePool) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, StorageError>> + Send + 'static,
    {
        let (reply, rx) = std::sync::mpsc::channel::<Result<T, StorageError>>();
        let job: Job = Box::new(move |pool| {
            Box::pin(async move {
                let _ = reply.send(f(pool).await);
            })
        });
        self.tx
            .send(job)
            .map_err(|_| StorageError::Unavailable("engine store task is gone".into()))?;
        rx.recv_timeout(ENGINE_BRIDGE_TIMEOUT)
            .map_err(|e| StorageError::Unavailable(format!("engine store did not answer: {e}")))?
    }
}

impl EngineStorage for ToronEngineStore {
    fn get_run(&self, key: &str) -> Result<Option<RunRow>, StorageError> {
        let key = key.to_string();
        self.call(move |pool| async move {
            let row = sqlx::query(
                "SELECT run_key, phase, generation, until_ms, attempt \
                 FROM durable_engine_runs WHERE run_key = ?1",
            )
            .bind(&key)
            .fetch_optional(&pool)
            .await
            .map_err(storage_error)?;
            row.as_ref().map(run_from_row).transpose()
        })
    }

    fn put_run(&mut self, row: RunRow) -> Result<(), StorageError> {
        self.call(move |pool| async move {
            let generation = i64::try_from(row.generation)
                .map_err(|_| StorageError::Corrupt("generation exceeds i64".into()))?;
            let until = row
                .until_ms
                .map(i64::try_from)
                .transpose()
                .map_err(|_| StorageError::Corrupt("until_ms exceeds i64".into()))?;
            let now = crate::index::workflow_effects::now_us();
            let mut tx = pool.begin().await.map_err(storage_error)?;
            upsert_run(
                &mut tx,
                &row.key,
                phase_str(row.phase),
                generation,
                until,
                row.attempt,
                now,
            )
            .await?;
            tx.commit().await.map_err(storage_error)?;
            Ok(())
        })
    }

    fn list_parked(&self) -> Result<Vec<ParkedRow>, StorageError> {
        self.call(move |pool| async move {
            let rows = sqlx::query(
                "SELECT run_key, until_ms, park_generation FROM durable_engine_parks \
                 ORDER BY until_ms, run_key",
            )
            .fetch_all(&pool)
            .await
            .map_err(storage_error)?;
            rows.iter().map(park_from_row).collect()
        })
    }

    fn get_park(&self, key: &str) -> Result<Option<ParkedRow>, StorageError> {
        let key = key.to_string();
        self.call(move |pool| async move {
            let row = sqlx::query(
                "SELECT run_key, until_ms, park_generation FROM durable_engine_parks \
                 WHERE run_key = ?1 ORDER BY until_ms LIMIT 1",
            )
            .bind(&key)
            .fetch_optional(&pool)
            .await
            .map_err(storage_error)?;
            row.as_ref().map(park_from_row).transpose()
        })
    }

    fn due_parks(&self, now_ms: u64) -> Result<Vec<ParkedRow>, StorageError> {
        let now = i64::try_from(now_ms)
            .map_err(|_| StorageError::Corrupt("now_ms exceeds i64".into()))?;
        self.call(move |pool| async move {
            let rows = sqlx::query(
                "SELECT run_key, until_ms, park_generation FROM durable_engine_parks \
                 WHERE until_ms <= ?1 ORDER BY until_ms, run_key",
            )
            .bind(now)
            .fetch_all(&pool)
            .await
            .map_err(storage_error)?;
            rows.iter().map(park_from_row).collect()
        })
    }

    fn upsert_park(&mut self, row: ParkedRow) -> Result<bool, StorageError> {
        self.call(move |pool| async move {
            let until = i64::try_from(row.until_ms)
                .map_err(|_| StorageError::Corrupt("until_ms exceeds i64".into()))?;
            let generation = i64::try_from(row.park_generation)
                .map_err(|_| StorageError::Corrupt("park_generation exceeds i64".into()))?;
            let now = crate::index::workflow_effects::now_us();
            let mut tx = pool.begin().await.map_err(storage_error)?;
            let existed: Option<(i64,)> = sqlx::query_as(
                "SELECT 1 FROM durable_engine_parks WHERE run_key = ?1 AND until_ms = ?2",
            )
            .bind(&row.key)
            .bind(until)
            .fetch_optional(&mut *tx)
            .await
            .map_err(storage_error)?;
            sqlx::query("DELETE FROM durable_engine_parks WHERE run_key = ?1")
                .bind(&row.key)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
            sqlx::query(
                "INSERT INTO durable_engine_parks \
                 (run_key, until_ms, park_generation, updated_at) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(&row.key)
            .bind(until)
            .bind(generation)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
            tx.commit().await.map_err(storage_error)?;
            Ok(existed.is_none())
        })
    }

    fn clear_parks(&mut self, key: &str) -> Result<(), StorageError> {
        let key = key.to_string();
        self.call(move |pool| async move {
            sqlx::query("DELETE FROM durable_engine_parks WHERE run_key = ?1")
                .bind(&key)
                .execute(&pool)
                .await
                .map_err(storage_error)?;
            Ok(())
        })
    }

    fn commit_run(&mut self, run: RunRow) -> Result<(), StorageError> {
        self.call(move |pool| async move {
            let generation = i64::try_from(run.generation)
                .map_err(|_| StorageError::Corrupt("generation exceeds i64".into()))?;
            let until = run
                .until_ms
                .map(i64::try_from)
                .transpose()
                .map_err(|_| StorageError::Corrupt("until_ms exceeds i64".into()))?;
            let phase = phase_str(run.phase);
            let now = crate::index::workflow_effects::now_us();
            let mut tx = pool.begin().await.map_err(storage_error)?;
            upsert_run(
                &mut tx,
                &run.key,
                phase,
                generation,
                until,
                run.attempt,
                now,
            )
            .await?;
            sqlx::query("DELETE FROM durable_engine_parks WHERE run_key = ?1")
                .bind(&run.key)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
            tx.commit().await.map_err(storage_error)?;
            Ok(())
        })
    }

    fn commit_park(&mut self, run: RunRow, park: ParkedRow) -> Result<(), StorageError> {
        self.call(move |pool| async move {
            let generation = i64::try_from(run.generation)
                .map_err(|_| StorageError::Corrupt("generation exceeds i64".into()))?;
            let until = run
                .until_ms
                .map(i64::try_from)
                .transpose()
                .map_err(|_| StorageError::Corrupt("until_ms exceeds i64".into()))?;
            let park_until = i64::try_from(park.until_ms)
                .map_err(|_| StorageError::Corrupt("until_ms exceeds i64".into()))?;
            let park_generation = i64::try_from(park.park_generation)
                .map_err(|_| StorageError::Corrupt("park_generation exceeds i64".into()))?;
            let phase = phase_str(run.phase);
            let now = crate::index::workflow_effects::now_us();
            let mut tx = pool.begin().await.map_err(storage_error)?;
            sqlx::query("DELETE FROM durable_engine_parks WHERE run_key = ?1")
                .bind(&park.key)
                .execute(&mut *tx)
                .await
                .map_err(storage_error)?;
            sqlx::query(
                "INSERT INTO durable_engine_parks \
                 (run_key, until_ms, park_generation, updated_at) VALUES (?1, ?2, ?3, ?4)",
            )
            .bind(&park.key)
            .bind(park_until)
            .bind(park_generation)
            .bind(now)
            .execute(&mut *tx)
            .await
            .map_err(storage_error)?;
            upsert_run(
                &mut tx,
                &run.key,
                phase,
                generation,
                until,
                run.attempt,
                now,
            )
            .await?;
            tx.commit().await.map_err(storage_error)?;
            Ok(())
        })
    }
}

async fn upsert_run(
    tx: &mut sqlx::SqliteConnection,
    key: &str,
    phase: &'static str,
    generation: i64,
    until: Option<i64>,
    attempt: i64,
    now: i64,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO durable_engine_runs \
         (run_key, phase, generation, until_ms, attempt, updated_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6) \
         ON CONFLICT(run_key) DO UPDATE SET phase = excluded.phase, \
         generation = excluded.generation, until_ms = excluded.until_ms, \
         attempt = excluded.attempt, updated_at = excluded.updated_at",
    )
    .bind(key)
    .bind(phase)
    .bind(generation)
    .bind(until)
    .bind(attempt)
    .bind(now)
    .execute(&mut *tx)
    .await
    .map_err(storage_error)?;
    Ok(())
}

fn phase_str(phase: RunPhase) -> &'static str {
    match phase {
        RunPhase::Running => "running",
        RunPhase::Parked => "parked",
        RunPhase::Cancelled => "cancelled",
        RunPhase::Settled => "settled",
        RunPhase::Failed => "failed",
        RunPhase::Unknown => "unknown",
    }
}

fn parse_phase(value: &str) -> Result<RunPhase, StorageError> {
    match value {
        "running" => Ok(RunPhase::Running),
        "parked" => Ok(RunPhase::Parked),
        "cancelled" => Ok(RunPhase::Cancelled),
        "settled" => Ok(RunPhase::Settled),
        "failed" => Ok(RunPhase::Failed),
        "unknown" => Ok(RunPhase::Unknown),
        other => Err(StorageError::Corrupt(format!(
            "unknown run phase {other:?}"
        ))),
    }
}

fn run_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<RunRow, StorageError> {
    let phase: String = row.try_get("phase").map_err(storage_error)?;
    let generation: i64 = row.try_get("generation").map_err(storage_error)?;
    let until: Option<i64> = row.try_get("until_ms").map_err(storage_error)?;
    Ok(RunRow {
        key: row.try_get("run_key").map_err(storage_error)?,
        phase: parse_phase(&phase)?,
        generation: u64::try_from(generation)
            .map_err(|_| StorageError::Corrupt("negative generation".into()))?,
        until_ms: until
            .map(u64::try_from)
            .transpose()
            .map_err(|_| StorageError::Corrupt("negative until_ms".into()))?,
        attempt: row.try_get("attempt").map_err(storage_error)?,
    })
}

fn park_from_row(row: &sqlx::sqlite::SqliteRow) -> Result<ParkedRow, StorageError> {
    let until: i64 = row.try_get("until_ms").map_err(storage_error)?;
    let generation: i64 = row.try_get("park_generation").map_err(storage_error)?;
    Ok(ParkedRow {
        key: row.try_get("run_key").map_err(storage_error)?,
        until_ms: u64::try_from(until)
            .map_err(|_| StorageError::Corrupt("negative until_ms".into()))?,
        park_generation: u64::try_from(generation)
            .map_err(|_| StorageError::Corrupt("negative park_generation".into()))?,
    })
}

fn storage_error(error: sqlx::Error) -> StorageError {
    if matches!(&error, sqlx::Error::Decode(_)) {
        StorageError::Corrupt(error.to_string())
    } else {
        StorageError::Unavailable(error.to_string())
    }
}
