//! Bead-tracker reads for lease audits (bd-close-releases-leases-qkbo).
//!
//! `.beads/issues.jsonl` is br's exported issue plane: `status` is
//! authoritative for ISSUES there (gate verdicts live in `gates.jsonl` and are
//! never consulted here). Best-effort by contract: a missing file or a
//! malformed line degrades to fewer known-closed beads, so an audit can add a
//! stale marker but never block `reserve list` / `reserve sweep`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Path of the tracker this module reads: `<repo>/.beads/issues.jsonl`.
pub fn issues_path(repo: &Path) -> PathBuf {
    repo.join(".beads").join("issues.jsonl")
}

/// Closed bead ids from `<repo>/.beads/issues.jsonl` (empty when absent).
pub fn closed_bead_ids(repo: &Path) -> BTreeSet<String> {
    closed_bead_ids_at(&issues_path(repo))
}

/// Closed bead ids from one tracker file (empty when absent or unreadable).
/// The explicit path is the form `reserve sweep --closed-beads <path>` takes.
pub fn closed_bead_ids_at(path: &Path) -> BTreeSet<String> {
    let Ok(raw) = std::fs::read_to_string(path) else {
        return BTreeSet::new();
    };
    let mut closed = BTreeSet::new();
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || !line.contains("closed") {
            continue;
        }
        let Ok(row) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if row.get("status").and_then(serde_json::Value::as_str) != Some("closed") {
            continue;
        }
        if let Some(id) = row
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| !id.is_empty())
        {
            closed.insert(id.to_string());
        }
    }
    closed
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_repo(tag: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "toron-tracker-{tag}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join(".beads")).expect("mkdir .beads");
        root
    }

    /// Only rows that are genuinely closed count: open/deferred/no-status
    /// rows, malformed lines, and a closed row with no id are all skipped —
    /// a stale audit must never mark a lease from a misread row.
    #[test]
    fn closed_bead_ids_reads_only_closed_rows() {
        let root = temp_repo("closed");
        let rows = [
            r#"{"id":"bd-open","status":"open"}"#,
            r#"{"id":"bd-closed","status":"closed","close_reason":"done"}"#,
            r#"{"id":"bd-deferred","status":"deferred"}"#,
            "not json at all",
            r#"{"id":"bd-no-status"}"#,
            r#"{"status":"closed"}"#,
            r#"{"id":"","status":"closed"}"#,
        ];
        std::fs::write(issues_path(&root), rows.join("\n")).expect("write tracker");
        let closed = closed_bead_ids(&root);
        assert_eq!(
            closed,
            ["bd-closed".to_string()]
                .into_iter()
                .collect::<BTreeSet<_>>(),
            "only the closed row with a non-empty id"
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    /// A repo without a tracker degrades to "no closed beads", never an
    /// error: `reserve list` must work everywhere.
    #[test]
    fn closed_bead_ids_missing_file_is_empty() {
        let root = temp_repo("missing");
        assert!(closed_bead_ids(&root).is_empty());
        let _ = std::fs::remove_dir_all(&root);
    }
}
