// governed-by: ADR-0003
#![forbid(unsafe_code)]

//! Infrastructure cluster (bd-led.5.1): `health_check`, `ensure_project`,
//! `install_precommit_guard`, `uninstall_precommit_guard` (the latter two live
//! in [`super::guard`], wired here as `Mailbox` methods).
//!
//! AM-Rust parity contract (ADR-0003 §Tool → Nostr mapping):
//! - `health_check` → bus liveness (`Bus::health`) + archive health.
//! - `ensure_project` → idempotent project dir + kind-30010 manifest publish.
//! - guard install/uninstall → libgit2 path; block foreign reserved paths.

use std::path::Path;

use serde::{Deserialize, Serialize};

use nostr_sdk::nostr::{Tag, Timestamp};

use crate::bus::filter::kind_project_filter;
use crate::bus::scoping::with_project_scoping;
use crate::nostr::event::build_event;
use crate::nostr::kinds::{PROJECT_MANIFEST, toron_kind};

use super::{Mailbox, collect_replay};

/// Health report — the stable JSON shape of the `health_check` tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthReport {
    /// Bus liveness + event store status.
    pub bus: HealthComponent,
    /// Archive health (git repo + commit count).
    pub archive: HealthComponent,
}

/// A single health component's status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthComponent {
    pub ok: bool,
    pub message: String,
}

/// Result of `ensure_project`: idempotent — the second call for an existing
/// project is a no-op (returns the same coordinate + manifest event id).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectState {
    /// The project slug (== `d` tag of the kind-30010 manifest).
    pub slug: String,
    /// Event id of the published kind-30010 manifest (empty on existing).
    pub manifest_event_id: String,
    /// True when this call created the project; false when it already existed.
    pub created: bool,
}

impl Mailbox {
    /// `health_check`: bus liveness + archive health as a stable JSON shape.
    pub async fn health_check(&self) -> HealthReport {
        let bus_health = match self.bus.health().await {
            Ok(h) => HealthComponent {
                ok: h.ok,
                message: h.message,
            },
            Err(e) => HealthComponent {
                ok: false,
                message: format!("bus error: {e}"),
            },
        };

        let archive = HealthComponent {
            ok: self.data_dir.exists(),
            message: if self.data_dir.exists() {
                format!("data dir present: {}", self.data_dir.display())
            } else {
                format!("data dir missing: {}", self.data_dir.display())
            },
        };

        HealthReport {
            bus: bus_health,
            archive,
        }
    }

    /// `ensure_project`: idempotent project initialization.
    ///
    /// Publishes the operator-signed kind-30010 project manifest (`d`=slug)
    /// with dual-emit scoping tags (`#t` + NIP-32). On an existing project
    /// (manifest already present) it is a no-op: it re-reads the stored
    /// manifest and returns `created: false` — AM-Rust "idempotent
    /// bootstrapping" parity (ADR-0003 Invariants).
    pub async fn ensure_project(&self, slug: &str) -> anyhow::Result<ProjectState> {
        std::fs::create_dir_all(self.data_dir.join(slug))?;

        let existing = self
            .find_manifest(slug)
            .await?
            .map(|e| e.id.to_hex())
            .unwrap_or_default();

        if !existing.is_empty() {
            return Ok(ProjectState {
                slug: slug.to_string(),
                manifest_event_id: existing,
                created: false,
            });
        }

        let operator = self.operator.as_ref().ok_or_else(|| {
            anyhow::anyhow!("no operator key configured; cannot sign project manifest")
        })?;

        let base_tags = vec![Tag::parse(["d", slug]).expect("d tag")];
        let tags = with_project_scoping(toron_kind(PROJECT_MANIFEST), base_tags, Some(slug));
        let event = build_event(
            operator,
            toron_kind(PROJECT_MANIFEST),
            "{}",
            tags,
            Timestamp::now(),
        )?;
        let ok = self.bus.publish(event.clone()).await?;
        if !ok.stored {
            // A concurrent ensure_project won the race; treat as existing.
            return Ok(ProjectState {
                slug: slug.to_string(),
                manifest_event_id: event.id.to_hex(),
                created: false,
            });
        }

        Ok(ProjectState {
            slug: slug.to_string(),
            manifest_event_id: event.id.to_hex(),
            created: true,
        })
    }

    /// Find the stored kind-30010 manifest for `slug` (project-scoped `#t`).
    /// Addressable latest-wins per author; any kind-30010 with `d`=slug is the
    /// manifest, so the newest stored event wins.
    async fn find_manifest(&self, slug: &str) -> anyhow::Result<Option<nostr_sdk::nostr::Event>> {
        let filter = kind_project_filter(toron_kind(PROJECT_MANIFEST), slug);
        let events = collect_replay(self.bus.as_ref(), filter).await?;
        Ok(events.into_iter().max_by_key(|e| e.created_at.as_secs()))
    }

    /// `install_precommit_guard`: install the toron pre-commit hook (libgit2
    /// path). Blocked commits = staged paths under ANOTHER agent's exclusive
    /// reservation. The project slug is baked into the hook script so the
    /// guard evaluates the right project's reservations.
    pub fn install_precommit_guard(
        &self,
        repo: &Path,
        project: &str,
    ) -> anyhow::Result<std::path::PathBuf> {
        crate::mailbox::guard::install_precommit_guard(repo, project)
    }

    /// `uninstall_precommit_guard`: remove the toron hook (idempotent; never
    /// clobbers a user hook).
    pub fn uninstall_precommit_guard(&self, repo: &Path) -> anyhow::Result<bool> {
        crate::mailbox::guard::uninstall_precommit_guard(repo)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::bus::SubscriptionEvent;
    use crate::bus::in_process::{InProcessBus, InProcessBusConfig};
    use crate::index::schema;
    use crate::keys;
    use nostr_sdk::nostr::{Keys, SubscriptionId, Timestamp};
    use sqlx::SqlitePool;

    async fn test_mailbox(
        tag: &str,
    ) -> (Mailbox, SqlitePool, std::path::PathBuf, std::path::PathBuf) {
        let db_path =
            std::env::temp_dir().join(format!("toron-mb-{tag}-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-mb-data-{tag}-{}", uuid::Uuid::new_v4()));
        let operator = keys::generate();
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), Some(operator));
        (mailbox, pool, db_path, data_dir)
    }

    async fn drain_sub(
        sub: &mut crate::bus::Subscription,
        max: usize,
        silence_ms: u64,
    ) -> Vec<crate::bus::SubscriptionEvent> {
        let mut out = Vec::new();
        for _ in 0..max {
            match tokio::time::timeout(std::time::Duration::from_millis(silence_ms), sub.next())
                .await
            {
                Ok(Some(ev)) => out.push(ev),
                _ => break,
            }
        }
        out
    }

    #[tokio::test]
    async fn test_ensure_project_twice_is_noop() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("proj").await;

        let first = mailbox.ensure_project("demo").await.expect("first");
        assert!(first.created, "first call creates the project");
        assert_eq!(first.slug, "demo");
        assert!(!first.manifest_event_id.is_empty());

        let second = mailbox.ensure_project("demo").await.expect("second");
        assert!(!second.created, "second call is a no-op");
        assert_eq!(
            second.manifest_event_id, first.manifest_event_id,
            "same manifest re-read, not republished"
        );

        // Only ONE kind-30010 manifest exists in the bus for "demo".
        let filter = kind_project_filter(toron_kind(PROJECT_MANIFEST), "demo");
        let manifest_count = {
            let bus = mailbox.bus.clone();
            let mut sub = bus.subscribe(filter).await.expect("sub");
            drain_sub(&mut sub, 8, 200)
                .await
                .into_iter()
                .filter(|it| matches!(it, SubscriptionEvent::Event(_)))
                .count()
        };
        assert_eq!(manifest_count, 1, "exactly one manifest event stored");

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_ensure_project_writes_scoped_manifest() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("scoped").await;
        mailbox.ensure_project("demo").await.expect("ensure");

        // Manifest event must carry dual-emit scoping (#t + NIP-32 label).
        let filter = kind_project_filter(toron_kind(PROJECT_MANIFEST), "demo");
        let events = collect_replay(mailbox.bus.as_ref(), filter)
            .await
            .expect("collect");
        assert_eq!(events.len(), 1);
        let ev = &events[0];
        assert_eq!(ev.kind.as_u16(), PROJECT_MANIFEST);
        assert!(
            ev.tags
                .iter()
                .any(|t| t.as_slice() == ["t".to_string(), "demo".to_string()])
        );
        assert!(ev
            .tags
            .iter()
            .any(|t| t.as_slice() == ["l".to_string(), "demo".to_string(), "toron".to_string()]));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_health_check_json_shape_stable() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("health").await;

        let report = mailbox.health_check().await;
        // JSON shape: { bus: {ok, message}, archive: {ok, message} }.
        let json = serde_json::to_value(&report).expect("serialize");
        assert_eq!(json["bus"]["ok"], true, "bus healthy");
        assert!(json["bus"]["message"].is_string());
        assert!(json["archive"]["ok"].is_boolean());
        assert!(json["archive"]["message"].is_string());

        // Round-trips: deserialize is stable (schema lock).
        let back: HealthReport = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, report);

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    #[tokio::test]
    async fn test_guard_install_uninstall_through_mailbox() {
        let (mailbox, _pool, db_path, data_dir) = test_mailbox("guard").await;

        let repo_root =
            std::env::temp_dir().join(format!("toron-mb-guard-{}.git", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&repo_root).expect("repo dir");
        let _repo = git2::Repository::init(&repo_root).expect("init");

        let hook = mailbox
            .install_precommit_guard(&repo_root, "demo")
            .expect("install");
        assert!(hook.exists());

        let removed = mailbox
            .uninstall_precommit_guard(&repo_root)
            .expect("uninstall");
        assert!(removed);
        assert!(!hook.exists());

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
        let _ = std::fs::remove_dir_all(&repo_root);
    }

    #[tokio::test]
    async fn test_ensure_project_requires_operator() {
        let db_path =
            std::env::temp_dir().join(format!("toron-mb-nop-{}.sqlite", uuid::Uuid::new_v4()));
        let pool = schema::connect(db_path.to_str().unwrap())
            .await
            .expect("connect");
        let bus = InProcessBus::open(pool.clone(), InProcessBusConfig::default())
            .await
            .expect("bus");
        let data_dir =
            std::env::temp_dir().join(format!("toron-mb-data-nop-{}", uuid::Uuid::new_v4()));
        let mailbox = Mailbox::new(Arc::new(bus), data_dir.clone(), None);

        let err = mailbox
            .ensure_project("demo")
            .await
            .expect_err("no operator");
        assert!(err.to_string().contains("operator key"));

        let _ = std::fs::remove_file(&db_path);
        let _ = std::fs::remove_dir_all(&data_dir);
    }

    // Compile-only: unused warning guard for helper imports used in tests.
    #[allow(dead_code)]
    fn _keep(_: &Keys, _: SubscriptionId, _: Timestamp) {}
}
