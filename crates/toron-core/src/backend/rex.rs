//! Rex session/block projectors onto the hc shapes flywheel already
//! consumes (bd-i328).
//!
//! Pure by contract: no process spawn, no live Rex, no I/O. Callers hand in
//! ids and the terminal's status records; these functions only map them.
//!
//! governed-by: ADR-0002

use crate::backend::AgentStatus;
use crate::backend::{PaneInfo, WorkspaceInfo};
use crate::osc7501::{Board, parse_report};
use serde_json::Value;

/// A Rex session is an hc workspace; the session id is the workspace id.
/// Its agent status is `Unknown`: a session with no records proves nothing.
pub fn session_to_workspace(session_id: &str, label: &str) -> WorkspaceInfo {
    WorkspaceInfo {
        workspace_id: session_id.to_string(),
        number: 0,
        label: label.to_string(),
        focused: false,
        pane_count: 0,
        tab_count: 0,
        active_tab_id: String::new(),
        agent_status: AgentStatus::Unknown,
        cwd: None,
    }
}

/// A Rex block is an hc pane: block id is the pane id, session id is the
/// workspace id, and a Rex window is the tab. `terminal_id` stays empty —
/// Rex has no second terminal handle beyond the block.
pub fn block_to_pane(session_id: &str, block_id: &str, label: &str, window_id: &str) -> PaneInfo {
    PaneInfo {
        pane_id: block_id.to_string(),
        terminal_id: String::new(),
        workspace_id: session_id.to_string(),
        tab_id: window_id.to_string(),
        focused: false,
        agent_status: AgentStatus::Unknown,
        revision: 0,
        title: None,
        cwd: None,
        agent: None,
        agent_session: None,
        display_agent: None,
        foreground_cwd: None,
        label: Some(label.to_string()),
        scroll: None,
        state_labels: serde_json::Map::new(),
        terminal_title: None,
        terminal_title_stripped: None,
        tokens: serde_json::Map::new(),
    }
}

/// Fold one terminal's records into a single status.
///
/// `records_json` is a JSON array (or a bare record) whose items are either
/// raw OSC 7501 strings or objects carrying `state` (and optionally `id`).
/// Both go through `osc7501`, so string and object records obey one grammar:
/// a discarded report changes nothing, and `clear` removes records instead of
/// being stored.
///
/// Rex `idle` is at-a-prompt and is reported as `Idle` here; `error` is
/// `Unknown`, never `Done` and never `Idle`.
pub fn status_from_records(records_json: &str) -> AgentStatus {
    let Ok(value) = serde_json::from_str::<Value>(records_json) else {
        return AgentStatus::Unknown;
    };
    let items = match value {
        Value::Array(items) => items,
        other => vec![other],
    };
    let mut board = Board::default();
    let mut order: Vec<String> = Vec::new();
    for item in &items {
        apply_record(&mut board, &mut order, item);
    }
    fold_status(&board, &order)
}

/// Apply one JSON record to the board. Strings are raw OSC 7501 reports;
/// objects carrying `state` (and optionally `id`) render through the same
/// grammar, so an id outside the OSC id grammar drops the record, same as
/// a malformed raw string. A discarded report changes nothing.
fn apply_record(board: &mut Board, order: &mut Vec<String>, item: &Value) {
    let raw;
    let text = match item {
        Value::String(text) => text.as_str(),
        Value::Object(map) => {
            let Some(state) = map.get("state").and_then(Value::as_str) else {
                return;
            };
            let id = map.get("id").and_then(Value::as_str).unwrap_or_default();
            raw = if id.is_empty() {
                format!("\u{1b}]7501;state={state}\u{1b}\\")
            } else {
                format!("\u{1b}]7501;state={state}:id={id}\u{1b}\\")
            };
            raw.as_str()
        }
        _ => return,
    };
    let Some(report) = parse_report(text) else {
        return;
    };
    order.push(report.id.clone().unwrap_or_default());
    board.apply(text);
}

/// Fold one terminal's board into a single status. Pure: no I/O, no clock.
///
/// Attention policy (shared with hc via `crate::osc7501::fold`): a
/// blocked child outranks a working root. RootWins is not used here.
pub fn fold_status(board: &Board, _order: &[String]) -> AgentStatus {
    crate::osc7501::fold(board, crate::osc7501::FoldPolicy::Attention)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn osc(body: &str) -> String {
        format!("\u{1b}]7501;{body}\u{1b}\\")
    }

    #[test]
    fn osc_error_is_not_done() {
        let failed = serde_json::json!([osc("state=error:app=freebuff")]).to_string();
        let status = status_from_records(&failed);
        assert_eq!(status, AgentStatus::Unknown);
        assert_ne!(status, AgentStatus::Done);
        assert_ne!(status, AgentStatus::Idle);

        assert_eq!(
            status_from_records(r#"[{"state":"error","app":"freebuff"}]"#),
            AgentStatus::Unknown
        );

        let idle = serde_json::json!([osc("state=idle:app=claude-code")]).to_string();
        assert_eq!(status_from_records(&idle), AgentStatus::Idle);
        assert_eq!(
            status_from_records(r#"[{"state":"idle"}]"#),
            AgentStatus::Idle
        );

        let blocked = serde_json::json!([osc("state=blocked:kind=permission")]).to_string();
        assert_eq!(status_from_records(&blocked), AgentStatus::Blocked);
        assert_eq!(
            status_from_records(r#"[{"state":"blocked"}]"#),
            AgentStatus::Blocked
        );

        assert_eq!(status_from_records("[]"), AgentStatus::Unknown);
        assert_eq!(status_from_records("not json"), AgentStatus::Unknown);
    }

    #[test]
    fn blocked_child_outranks_a_working_root() {
        let records = serde_json::json!([
            osc("state=working:app=deploy"),
            osc("state=blocked:id=eu-west:kind=permission"),
        ])
        .to_string();
        assert_eq!(status_from_records(&records), AgentStatus::Blocked);

        let child_only = serde_json::json!([
            osc("state=working:id=us-east"),
            osc("state=blocked:id=eu-west"),
        ])
        .to_string();
        assert_eq!(status_from_records(&child_only), AgentStatus::Blocked);

        let cleared = serde_json::json!([
            osc("state=blocked:id=eu-west"),
            osc("state=clear:id=eu-west"),
        ])
        .to_string();
        assert_eq!(status_from_records(&cleared), AgentStatus::Unknown);
    }

    #[test]
    fn session_and_block_project_onto_plane_shapes() {
        let workspace = session_to_workspace("sess-1", "rex-build");
        assert_eq!(workspace.workspace_id, "sess-1");
        assert_eq!(workspace.label, "rex-build");
        assert_eq!(workspace.agent_status, AgentStatus::Unknown);

        let pane = block_to_pane("sess-1", "blk-7", "cargo test", "win-2");
        assert_eq!(pane.pane_id, "blk-7");
        assert_eq!(pane.workspace_id, "sess-1");
        assert_eq!(pane.label.as_deref(), Some("cargo test"));
        assert_eq!(pane.tab_id, "win-2");
    }

    /// The toron-core contract this fold relies on: `plane_status` carries
    /// working/blocked/done and refuses idle/error/clear, so OSC idle never
    /// reads as finished-and-seen and OSC error never reads as done.
    #[test]
    fn toron_osc_plane_status_contract() {
        use crate::osc7501::parse_report;
        for (body, expected) in [
            ("state=working:app=rex", Some(AgentStatus::Working)),
            ("state=blocked:kind=permission", Some(AgentStatus::Blocked)),
            ("state=done:app=rex", Some(AgentStatus::Done)),
            ("state=idle:app=rex", None),
            ("state=error:app=freebuff", None),
            ("state=clear:id=eu-west", None),
        ] {
            let report = parse_report(&osc(body)).expect("spec-shaped report parses");
            assert_eq!(report.osc_status(), expected, "{body}");
        }
        assert!(parse_report("not a report").is_none());
    }
}
