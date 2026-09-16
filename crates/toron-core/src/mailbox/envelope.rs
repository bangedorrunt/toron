// governed-by: ADR-0031
//! Persist-envelope cross-check (ADR-0031 XState persist, bead bd-eg09).
//!
//! Relearned from source (`packages/core`): XState core persistence is
//! snapshot passthrough per logic (`getPersistedSnapshot` /
//! `restoreSnapshot`) — versioning, write-ahead, and torn-tail handling are
//! the ENVELOPE's job, which is exactly what journal v2 must define.
//!
//! The versioned envelope itself is flywheel's journal v2 scope; toron's
//! share is the durable contract v2 builds on (receipts `owed→acked→
//! resulted` monotonicity + bus sqlite as the cross-process share). This
//! module pins toron's side so the v2 checklist is asserted, not prose:
//!
//! - [`EnvelopeRow`]: version-tolerant row shape — a missing `v` reads as 1
//!   (old rows stay readable); unknown states never parse.
//! - [`fold_strict`]: strict replay — write-ahead ordering (acked/degraded
//!   need a prior owed for the corr; `resulted` is the explicit verb and may
//!   create), monotonicity mirroring the store guards (downgrades ignored,
//!   never applied), sequence continuity when `seq`s are present. The first
//!   gap stops loudly with the row index (torn-tail rule) — never skipped.
//!
//! Comparison table (XState vs journal v2 vs toron) lives on the bead
//! thread; gaps v2 owns are follow-on beads, not silent absorbs.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::receipts::ReceiptState;

/// Current envelope version.
pub const ENVELOPE_VERSION: u32 = 1;

const fn v1() -> u32 {
    1
}

/// Versioned log row. `v` defaults to 1 when absent (reads old rows);
/// `state` parses strictly on fold (unknown states stop the replay).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvelopeRow {
    #[serde(default = "v1")]
    pub v: u32,
    pub corr: String,
    pub state: String,
    #[serde(default)]
    pub seq: Option<u64>,
}

impl EnvelopeRow {
    pub fn new(corr: &str, state: ReceiptState, seq: Option<u64>) -> Self {
        Self {
            v: ENVELOPE_VERSION,
            corr: corr.to_string(),
            state: state.as_str().to_string(),
            seq,
        }
    }
}

/// Strict replay of an envelope row stream. Returns the folded corr→state
/// map. Errors name the row index: unknown state, acked/degraded with no
/// prior owed for the corr (write-ahead), or a `seq` discontinuity
/// (torn-tail). Downgrades are ignored, mirroring the store guards — the
/// fold result is monotonic by construction.
pub fn fold_strict(rows: &[EnvelopeRow]) -> Result<HashMap<String, ReceiptState>, String> {
    let mut states: HashMap<String, ReceiptState> = HashMap::new();
    let mut last_seq: Option<u64> = None;
    for (i, row) in rows.iter().enumerate() {
        if let Some(seq) = row.seq {
            if let Some(prev) = last_seq
                && seq != prev + 1
            {
                return Err(format!(
                    "row {i}: seq gap {prev} → {seq} — refusing to fold past the torn tail"
                ));
            }
            last_seq = Some(seq);
        }
        let state = ReceiptState::parse(&row.state)
            .map_err(|_| format!("row {i}: unknown state `{}` — refusing to fold", row.state))?;
        match (states.get(&row.corr), state) {
            (None, ReceiptState::Owed) | (None, ReceiptState::Resulted) => {
                states.insert(row.corr.clone(), state);
            }
            (None, other) => {
                return Err(format!(
                    "row {i}: {} with no prior owed for `{}` — no delivery intent without a journaled row",
                    other.as_str(),
                    row.corr
                ));
            }
            (Some(ReceiptState::Resulted), _) => {} // terminal: ignore
            (Some(_), ReceiptState::Resulted) => {
                states.insert(row.corr.clone(), ReceiptState::Resulted);
            }
            (Some(ReceiptState::Acked), ReceiptState::Owed) => {} // ignore downgrade
            (Some(ReceiptState::Degraded), ReceiptState::Owed) => {} // never re-owed
            (Some(ReceiptState::Acked), ReceiptState::Degraded) => {} // ack wins
            (Some(_), next) => {
                states.insert(row.corr.clone(), next);
            }
        }
    }
    Ok(states)
}

// ponytail: table cases live in the fold tests; store tests pin the two
// guards v2 relies on. v2-owned gaps go to follow-on beads, not here.
#[cfg(test)]
mod journal_envelope_tests {
    use super::*;
    use crate::index::schema;
    use sqlx::SqlitePool;

    async fn test_pool(tag: &str) -> (SqlitePool, std::path::PathBuf) {
        let path = std::env::temp_dir().join(format!(
            "toron-envelope-{tag}-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let pool = schema::connect(path.to_str().unwrap())
            .await
            .expect("connect");
        schema::migrate(&pool).await.expect("migrate");
        (pool, path)
    }

    fn row(corr: &str, state: ReceiptState, seq: Option<u64>) -> EnvelopeRow {
        EnvelopeRow::new(corr, state, seq)
    }

    #[test]
    fn version_tolerance_missing_v_reads_as_1() {
        let v: EnvelopeRow =
            serde_json::from_str(r#"{"corr":"c1","state":"owed"}"#).expect("parses");
        assert_eq!(v.v, 1);
        assert_eq!(v.seq, None);
    }

    #[test]
    fn unknown_state_stops_loudly() {
        let rows = vec![
            row("c1", ReceiptState::Owed, Some(1)),
            EnvelopeRow {
                v: 1,
                corr: "c1".into(),
                state: "teleported".into(),
                seq: Some(2),
            },
        ];
        let err = fold_strict(&rows).expect_err("must stop");
        assert!(err.contains("row 1"), "names the index, got: {err}");
    }

    #[test]
    fn ack_without_prior_owed_stops_write_ahead() {
        let rows = vec![EnvelopeRow::new("c9", ReceiptState::Acked, Some(1))];
        let err = fold_strict(&rows).expect_err("write-ahead violation");
        assert!(err.contains("no prior owed"), "got: {err}");
    }

    #[test]
    fn seq_gap_stops_loudly_torn_tail() {
        let rows = vec![
            row("c1", ReceiptState::Owed, Some(7)),
            row("c1", ReceiptState::Acked, Some(9)),
        ];
        let err = fold_strict(&rows).expect_err("must stop at gap");
        assert!(err.contains("row 1") && err.contains("7 → 9"), "got: {err}");
    }

    #[test]
    fn fold_result_is_monotonic_by_construction() {
        let rows = vec![
            row("c1", ReceiptState::Owed, Some(1)),
            row("c1", ReceiptState::Acked, Some(2)),
            row("c1", ReceiptState::Owed, Some(3)), // ignored downgrade
            row("c1", ReceiptState::Resulted, Some(4)),
            row("c1", ReceiptState::Acked, Some(5)), // ignored past terminal
        ];
        let map = fold_strict(&rows).expect("folds");
        assert_eq!(map["c1"], ReceiptState::Resulted);
    }

    #[tokio::test]
    async fn store_ack_without_row_creates_nothing() {
        use super::super::receipts::ReceiptStore;
        let (pool, _path) = test_pool("no-row").await;
        let store = ReceiptStore::new(pool);
        assert!(!store.transition_acked("ghost").await.expect("ok"));
        assert!(store.get("ghost").await.expect("ok").is_none());
    }

    #[tokio::test]
    async fn store_guards_never_downgrade() {
        use super::super::receipts::{Receipt, ReceiptStore};
        let (pool, _path) = test_pool("guards").await;
        let store = ReceiptStore::new(pool);
        let now = chrono::Utc::now().timestamp_millis();
        store
            .set_resulted("c1", None)
            .await
            .expect("explicit verb creates");
        store
            .upsert(&Receipt {
                corr: "c1".into(),
                state: ReceiptState::Owed,
                wake: None,
                due_at: None,
                result_path: None,
                updated_at: now,
            })
            .await
            .expect("upsert ok (ignored)");
        let r = store.get("c1").await.expect("ok").expect("row");
        assert_eq!(r.state, ReceiptState::Resulted, "resulted never downgrades");
    }
}
