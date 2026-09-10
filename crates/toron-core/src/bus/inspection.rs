// governed-by: ADR-0031
//! Typed inspection events (ADR-0031 XState inspection protocol, bead bd-ii04).
//!
//! XState types every runtime event (`snapshot/event/actor/transition/
//! microstep/action`) for one inspector consumer. Our journal stages, audit
//! rows, SSE stream, and `why`-fold each grew their own vocabulary. This
//! module is the one taxonomy they share:
//!
//! - [`InspectionKind`]: `snapshot`, `transition` (Msg + state-delta +
//!   Effects triple), `microstep`, `action`, `degraded`.
//! - [`STAGE_MAP`]: rename table from every existing journal `stage:` string
//!   onto the taxonomy. Readers tolerate old stage-only rows (mapped here);
//!   writers emit [`InspectionEvent`] with a kind. Unknown kinds fail loudly
//!   — never skipped, never defaulted.
//! - [`why_fold`]: replays a mixed old/new sequence into one digest and
//!   stops loudly at gaps it cannot fold (no lying by omission).
//!
//! No new Nostr kinds, no MCP tools, no wire-format break: this is
//! host-side typing over existing rows.

use serde::{Deserialize, Serialize};

/// The five inspection kinds. New kinds need an ADR (same frozen-contract
/// instinct as the tool surface) — the validator rejects anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InspectionKind {
    Snapshot,
    Transition,
    Microstep,
    Action,
    Degraded,
}

impl InspectionKind {
    pub fn as_str(self) -> &'static str {
        match self {
            InspectionKind::Snapshot => "snapshot",
            InspectionKind::Transition => "transition",
            InspectionKind::Microstep => "microstep",
            InspectionKind::Action => "action",
            InspectionKind::Degraded => "degraded",
        }
    }

    fn parse(raw: &str) -> Option<InspectionKind> {
        match raw {
            "snapshot" => Some(InspectionKind::Snapshot),
            "transition" => Some(InspectionKind::Transition),
            "microstep" => Some(InspectionKind::Microstep),
            "action" => Some(InspectionKind::Action),
            "degraded" => Some(InspectionKind::Degraded),
            _ => None,
        }
    }
}

/// Rename table: every existing journal `stage:` string → taxonomy kind.
/// Pipeline stages (verify/auth/dedup/fanout) are `microstep`s of one
/// transition; hook boundaries are `action`s; anything failure-shaped is
/// `degraded` only when the detail says so (see [`InspectionEvent::for_stage`]).
pub const STAGE_MAP: &[(&str, InspectionKind)] = &[
    ("verify", InspectionKind::Microstep),
    ("auth", InspectionKind::Microstep),
    ("dedup", InspectionKind::Microstep),
    ("fanout", InspectionKind::Microstep),
    ("search_hook", InspectionKind::Action),
    ("audit_hook", InspectionKind::Action),
    ("workflow_hook", InspectionKind::Action),
];

/// One inspection event. `kind` is always present on newly written rows;
/// `stage` carries the legacy journal string for back-compat reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InspectionEvent {
    pub kind: InspectionKind,
    /// Legacy `stage:` string (`verify`, `fanout`, …). `None` only for
    /// synthetic events (snapshots) that never had a stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

impl InspectionEvent {
    /// Writer constructor: maps a journal stage string onto the taxonomy.
    /// Failure-shaped details (`failure`, `dropped`, `not allowlisted`)
    /// escalate the kind to `degraded` — the stage alone never proves health.
    pub fn for_stage(stage: &str, detail: &str) -> Self {
        let base = STAGE_MAP
            .iter()
            .find(|(s, _)| *s == stage)
            .map(|(_, k)| *k)
            .unwrap_or(InspectionKind::Action);
        let kind = if detail.contains("failure")
            || detail.contains("dropped")
            || detail.contains("not allowlisted")
        {
            InspectionKind::Degraded
        } else {
            base
        };
        Self {
            kind,
            stage: Some(stage.to_string()),
            detail: Some(detail.to_string()),
        }
    }

    /// Strict validation: every emitted event must carry a known kind.
    /// Unknown `kind` values fail — writers never emit them.
    pub fn validate(value: &serde_json::Value) -> Result<InspectionEvent, String> {
        let kind_raw = value
            .get("kind")
            .and_then(|k| k.as_str())
            .ok_or_else(|| "inspection event missing `kind`".to_string())?;
        let kind = InspectionKind::parse(kind_raw)
            .ok_or_else(|| format!("unknown inspection kind `{kind_raw}`"))?;
        Ok(InspectionEvent {
            kind,
            stage: value
                .get("stage")
                .and_then(|s| s.as_str())
                .map(str::to_string),
            detail: value
                .get("detail")
                .and_then(|d| d.as_str())
                .map(str::to_string),
        })
    }
}

/// Fold a mixed old/new journal into one digest. Old stage-only rows map via
/// [`STAGE_MAP`]; new rows validate strictly. Unknown kinds (or rows with
/// neither) STOP the fold with an error naming the index — `why` never lies
/// by omission.
pub fn why_fold(rows: &[serde_json::Value]) -> Result<String, String> {
    let mut out = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let ev = if row.get("kind").is_some() {
            InspectionEvent::validate(row)
                .map_err(|e| format!("row {i}: {e} — refusing to fold past the gap"))?
        } else if let Some(stage) = row.get("stage").and_then(|s| s.as_str()) {
            let detail = row.get("detail").and_then(|d| d.as_str()).unwrap_or("");
            InspectionEvent::for_stage(stage, detail)
        } else {
            return Err(format!(
                "row {i}: no `kind` and no `stage` — refusing to fold past the gap"
            ));
        };
        let detail = ev.detail.as_deref().unwrap_or("");
        out.push(format!(
            "{}:{}{}",
            ev.kind.as_str(),
            ev.stage.as_deref().unwrap_or("-"),
            if detail.is_empty() {
                String::new()
            } else {
                format!(" ({detail})")
            }
        ));
    }
    Ok(out.join(" → "))
}

// ponytail: one test module, table-driven over STAGE_MAP. New stages extend
// the table; the all-mapped test forces the taxonomy to follow.
#[cfg(test)]
mod inspection_events_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn every_stage_maps_onto_taxonomy() {
        for (stage, kind) in STAGE_MAP {
            let ev = InspectionEvent::for_stage(stage, "ok");
            assert_eq!(&ev.kind, kind, "stage `{stage}`");
            // Round-trips through the validator (writers emit valid rows).
            let v = serde_json::to_value(&ev).expect("serializes");
            assert_eq!(InspectionEvent::validate(&v).expect("valid"), ev);
        }
    }

    #[test]
    fn failure_detail_escalates_to_degraded() {
        assert_eq!(
            InspectionEvent::for_stage("dedup", "store failure: io").kind,
            InspectionKind::Degraded
        );
        assert_eq!(
            InspectionEvent::for_stage("dedup", "duplicate event id, dropped").kind,
            InspectionKind::Degraded
        );
        assert_eq!(
            InspectionEvent::for_stage("fanout", "no matching subscriber").kind,
            InspectionKind::Microstep
        );
    }

    #[test]
    fn unknown_kind_emission_fails() {
        let bad = json!({"kind": "teleport", "stage": "fanout"});
        assert!(
            InspectionEvent::validate(&bad).is_err(),
            "unknown kinds must fail, never default"
        );
        assert!(InspectionEvent::validate(&json!({"stage": "fanout"})).is_err());
    }

    #[test]
    fn why_fold_replays_mixed_old_new() {
        let rows = vec![
            json!({"stage": "verify", "detail": "ok"}),
            json!({"kind": "microstep", "stage": "dedup", "detail": "ok"}),
            json!({"kind": "action", "stage": "workflow_hook"}),
        ];
        let digest = why_fold(&rows).expect("mixed folds");
        assert!(digest.contains("microstep:verify"), "got: {digest}");
        assert!(digest.contains("action:workflow_hook"), "got: {digest}");
    }

    #[test]
    fn why_fold_stops_loudly_at_gaps() {
        let rows = vec![
            json!({"stage": "verify"}),
            json!({"kind": "teleport"}),
            json!({"stage": "fanout"}),
        ];
        let err = why_fold(&rows).expect_err("must stop at the gap");
        assert!(err.contains("row 1"), "names the index, got: {err}");
    }
}
