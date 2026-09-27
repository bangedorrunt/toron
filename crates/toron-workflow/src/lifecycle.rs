// governed-by: ADR-0003 Rev9 + ADR-0032 (GS-061 / Buzz #7735 workflow delete honesty)
#![forbid(unsafe_code)]

//! Workflow definition soft-delete under one lock (Buzz #7735 pattern).
//!
//! Soft-delete the definition **and** clear runnable/executable bits atomically
//! so a deleted workflow cannot ghost-run. Aligns with ADR-0032: journal
//! settlement ≠ business done — delete commits a tombstone effect; in-flight
//! runs observe `Deleted` and refuse new work without claiming provider-done.

use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

/// Soft-delete tombstone + executable flag for one workflow definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowRecord {
    /// Stable workflow id (name/slug).
    pub id: String,
    /// Soft-deleted?
    pub deleted: bool,
    /// Eligible to start new runs? Cleared atomically with delete.
    pub executable: bool,
    /// Optional generation for fence-on-reload.
    pub generation: u64,
}

/// Registry: definition soft-delete + executable bits under one mutex.
#[derive(Debug, Default)]
pub struct WorkflowLifecycle {
    inner: Mutex<HashMap<String, WorkflowRecord>>,
}

/// Outcome of a lifecycle mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LifecycleEffect {
    /// Registered as executable.
    Registered { id: String, generation: u64 },
    /// Soft-deleted; executable cleared under the same lock.
    SoftDeleted { id: String, generation: u64 },
    /// Already deleted — idempotent.
    AlreadyDeleted { id: String },
    /// Unknown id.
    NotFound { id: String },
    /// Refused to start: deleted or not executable.
    StartRefused { id: String, reason: &'static str },
    /// Start admitted.
    StartAdmitted { id: String, generation: u64 },
}

impl WorkflowLifecycle {
    /// Empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) a live executable definition.
    #[allow(clippy::significant_drop_tightening)]
    pub fn register(&self, id: impl Into<String>) -> LifecycleEffect {
        let id = id.into();
        let mut g = self.inner.lock().expect("lifecycle lock");
        let generation = g
            .get(&id)
            .map(|r| r.generation.saturating_add(1))
            .unwrap_or(1);
        g.insert(
            id.clone(),
            WorkflowRecord {
                id: id.clone(),
                deleted: false,
                executable: true,
                generation,
            },
        );
        LifecycleEffect::Registered { id, generation }
    }

    /// Soft-delete + clear executable under **one** lock (Buzz #7735).
    pub fn soft_delete(&self, id: &str) -> LifecycleEffect {
        let mut g = self.inner.lock().expect("lifecycle lock");
        match g.get_mut(id) {
            None => LifecycleEffect::NotFound { id: id.into() },
            Some(rec) if rec.deleted => LifecycleEffect::AlreadyDeleted { id: id.into() },
            Some(rec) => {
                rec.deleted = true;
                rec.executable = false; // same critical section — no ghost runnable
                LifecycleEffect::SoftDeleted {
                    id: id.into(),
                    generation: rec.generation,
                }
            }
        }
    }

    /// Admit a new run only if not deleted and still executable.
    pub fn try_start(&self, id: &str) -> LifecycleEffect {
        let g = self.inner.lock().expect("lifecycle lock");
        match g.get(id) {
            None => LifecycleEffect::NotFound { id: id.into() },
            Some(rec) if rec.deleted => LifecycleEffect::StartRefused {
                id: id.into(),
                reason: "workflow soft-deleted",
            },
            Some(rec) if !rec.executable => LifecycleEffect::StartRefused {
                id: id.into(),
                reason: "workflow not executable",
            },
            Some(rec) => LifecycleEffect::StartAdmitted {
                id: id.into(),
                generation: rec.generation,
            },
        }
    }

    /// Snapshot one record (tests / diagnostics).
    pub fn get(&self, id: &str) -> Option<WorkflowRecord> {
        self.inner.lock().expect("lifecycle lock").get(id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn soft_delete_clears_executable_under_one_lock() {
        let reg = WorkflowLifecycle::new();
        reg.register("wf");
        assert!(matches!(
            reg.soft_delete("wf"),
            LifecycleEffect::SoftDeleted { .. }
        ));
        let rec = reg.get("wf").expect("tombstone retained");
        assert!(rec.deleted);
        assert!(!rec.executable, "no ghost runnable after delete");
        assert!(matches!(
            reg.try_start("wf"),
            LifecycleEffect::StartRefused {
                reason: "workflow soft-deleted",
                ..
            }
        ));
    }

    #[test]
    fn soft_delete_is_idempotent() {
        let reg = WorkflowLifecycle::new();
        reg.register("wf");
        reg.soft_delete("wf");
        assert!(matches!(
            reg.soft_delete("wf"),
            LifecycleEffect::AlreadyDeleted { .. }
        ));
    }
}
