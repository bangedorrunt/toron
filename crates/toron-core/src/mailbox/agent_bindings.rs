// governed-by: ADR-0003
//! Shared pin → pane bindings (ADR-0019 D1/D6).
//!
//! Flywheel owns spawn/recover/rebind; toron-core owns identity reads. Before
//! this table the reader reached into `~/.local/state/flywheel/agents/*.json`,
//! a reverse dependency that made an in-process `Mailbox` resolve a different
//! world than the daemon. The shared index is now the authority.
//!
//! Writes are transactional: a pin's row is upserted, and any other pin
//! holding the same non-empty pane is removed in the same transaction. That
//! is the concurrent spawn/inject rule — a pane can never belong to two pins,
//! and a pin can never own two panes.

#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// One pin → pane binding. `pin` is the agent-mail identity (adjective+noun),
/// never the pane slug. Optional fields mirror flywheel's `SpawnEntry`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct AgentBinding {
    /// Project the binding belongs to.
    pub project_slug: String,
    /// Registered agent-mail identity.
    pub pin: String,
    /// Pane id (block id on rex, pane id on hc); empty for hosted bindings.
    pub pane_id: String,
    /// Pane/agent display name.
    pub name: String,
    /// Crew profile.
    pub profile: Option<String>,
    /// Harness.
    pub harness: Option<String>,
    /// Write-only model override (GS-043).
    pub model: Option<String>,
    /// Resolved Muse/slot pin.
    pub model_pin: Option<String>,
    /// Bounded skills digest.
    pub skills_digest: Option<String>,
    /// `pane` or `hosted`.
    pub binding_location: String,
    /// Harness session id.
    pub harness_session: Option<String>,
    /// Relaunch count.
    pub incarnation: i64,
    /// Whether the profile's skill prefix was injected.
    pub skills_injected: bool,
    /// Last write, unix micros.
    pub updated_ts_us: i64,
}

impl AgentBinding {
    /// New pane binding at the current host clock.
    pub fn pane(
        project_slug: impl Into<String>,
        pin: impl Into<String>,
        pane_id: impl Into<String>,
    ) -> Self {
        Self {
            project_slug: project_slug.into(),
            pin: pin.into(),
            pane_id: pane_id.into(),
            name: String::new(),
            profile: None,
            harness: None,
            model: None,
            model_pin: None,
            skills_digest: None,
            binding_location: "pane".to_string(),
            harness_session: None,
            incarnation: 0,
            skills_injected: false,
            updated_ts_us: 0,
        }
    }
}

/// Insert or replace one pin binding and clear any other pin on the same
/// non-empty pane. One transaction, so concurrent writers serialize on the
/// unique indexes instead of splitting a pane between two pins.
pub async fn upsert(
    pool: &sqlx::SqlitePool,
    project_slug: &str,
    binding: &AgentBinding,
    now_us: i64,
) -> anyhow::Result<()> {
    if binding.pin.trim().is_empty() {
        anyhow::bail!("agent binding pin is empty");
    }
    // Acquire the write lock before reading the conflicting pane. A deferred
    // transaction would let two spawns both observe the old owner and then
    // race the DELETE/INSERT; IMMEDIATE queues them behind the busy timeout.
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await?;
    if !binding.pane_id.is_empty() {
        sqlx::query(
            "DELETE FROM agent_bindings \
             WHERE project_slug = ?1 AND pane_id = ?2 AND pin <> ?3",
        )
        .bind(project_slug)
        .bind(&binding.pane_id)
        .bind(&binding.pin)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query(
        "INSERT INTO agent_bindings \
         (project_slug, pin, pane_id, name, profile, harness, model, model_pin, \
          skills_digest, binding_location, harness_session, incarnation, \
          skills_injected, updated_ts_us) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14) \
         ON CONFLICT (project_slug, pin) DO UPDATE SET \
           pane_id = excluded.pane_id, \
           name = excluded.name, \
           profile = excluded.profile, \
           harness = excluded.harness, \
           model = excluded.model, \
           model_pin = excluded.model_pin, \
           skills_digest = excluded.skills_digest, \
           binding_location = excluded.binding_location, \
           harness_session = excluded.harness_session, \
           incarnation = excluded.incarnation, \
           skills_injected = excluded.skills_injected, \
           updated_ts_us = excluded.updated_ts_us",
    )
    .bind(project_slug)
    .bind(&binding.pin)
    .bind(&binding.pane_id)
    .bind(&binding.name)
    .bind(&binding.profile)
    .bind(&binding.harness)
    .bind(&binding.model)
    .bind(&binding.model_pin)
    .bind(&binding.skills_digest)
    .bind(&binding.binding_location)
    .bind(&binding.harness_session)
    .bind(binding.incarnation)
    .bind(binding.skills_injected)
    .bind(now_us)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// All bindings for one project, ordered by pin (deterministic reads).
pub async fn list(
    pool: &sqlx::SqlitePool,
    project_slug: &str,
) -> anyhow::Result<Vec<AgentBinding>> {
    let rows = sqlx::query_as::<_, AgentBinding>(
        "SELECT project_slug, pin, pane_id, name, profile, harness, model, \
                model_pin, skills_digest, binding_location, harness_session, \
                incarnation, skills_injected, updated_ts_us \
         FROM agent_bindings WHERE project_slug = ?1 ORDER BY pin",
    )
    .bind(project_slug)
    .fetch_all(pool)
    .await?;
    Ok(rows)
}

/// Remove one pin binding.
pub async fn delete(pool: &sqlx::SqlitePool, project_slug: &str, pin: &str) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM agent_bindings WHERE project_slug = ?1 AND pin = ?2")
        .bind(project_slug)
        .bind(pin)
        .execute(pool)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn pool(tag: &str) -> (sqlx::SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "toron-agent-bindings-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = crate::index::schema::connect(path.to_str().expect("path"))
            .await
            .expect("connect");
        crate::index::schema::migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    #[tokio::test]
    async fn upsert_is_transactional_and_pane_is_exclusive() {
        let (pool, path) = pool("upsert").await;
        let first = AgentBinding::pane("proj", "BlueLake", "b:1");
        upsert(&pool, "proj", &first, 1).await.expect("first");
        let mut second = AgentBinding::pane("proj", "RoseDune", "b:1");
        second.profile = Some("coder".into());
        upsert(&pool, "proj", &second, 2).await.expect("second");

        let rows = list(&pool, "proj").await.expect("list");
        assert_eq!(rows.len(), 1, "one pane has one pin: {rows:?}");
        assert_eq!(rows[0].pin, "RoseDune");
        assert_eq!(rows[0].profile.as_deref(), Some("coder"));

        let other = AgentBinding::pane("proj", "BlueLake", "b:2");
        upsert(&pool, "proj", &other, 3).await.expect("rebind");
        let rows = list(&pool, "proj").await.expect("list");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].pane_id, "b:2");
        let _ = std::fs::remove_file(path);
    }

    /// The concurrent spawn/inject witness: eight writers claim the SAME
    /// pane with different pins at once. Whichever transaction lands last
    /// wins, but the table may never end with two pins on that pane.
    #[tokio::test]
    async fn concurrent_upserts_leave_one_pin_per_pane() {
        let (pool, path) = pool("concurrent").await;
        let pins = [
            "BlueLake",
            "RoseDune",
            "GreenMoss",
            "AmberFjord",
            "SilverReed",
            "CopperVale",
            "VioletPeak",
            "CrimsonBay",
        ];
        let mut tasks = Vec::new();
        for (i, pin) in pins.iter().enumerate() {
            let pool = pool.clone();
            let binding = AgentBinding::pane("proj", *pin, "b:shared");
            tasks.push(tokio::spawn(async move {
                upsert(&pool, "proj", &binding, i as i64).await
            }));
        }
        for task in tasks {
            task.await
                .expect("writer task must not panic")
                .expect("concurrent upsert must serialize");
        }

        let rows = list(&pool, "proj").await.expect("list");
        assert_eq!(rows.len(), 1, "one pane has exactly one pin: {rows:?}");
        assert_eq!(rows[0].pane_id, "b:shared");
        assert!(
            pins.contains(&rows[0].pin.as_str()),
            "winner is one of the racing pins: {rows:?}"
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn empty_pin_is_refused() {
        let (pool, path) = pool("empty").await;
        let bad = AgentBinding::pane("proj", " ", "b:1");
        assert!(upsert(&pool, "proj", &bad, 1).await.is_err());
        let _ = std::fs::remove_file(path);
    }
}
