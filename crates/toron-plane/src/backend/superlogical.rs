//! superlogical multiplexer backend over the `rex` CLI.
//!
//! governed-by: ADR-0002  ·  implements: `MultiplexerBackend`
//!
//! Session id is the workspace id. Block id is the pane id. The process
//! and the status fold live in `rex_cli` (the fold is `rex_cli::rex_map`).
//! This file only chooses the rex command. It never calls hc.
//!
//! The CLI layer is `crate::backend::rex_cli` — one module and one `RexFail`
//! type. Declaring the file here with `#[path]` compiled it twice and gave the
//! adapter a distinct type universe from the crate's public one.
//!
//! `subscribe`, `agent_wait`, and `agent_send_keys` stay on the trait
//! default, which names the method. A prompt's `wait` is not polled: that
//! would be `agent_wait`.
//!
//! Always compiled (ADR-0045): `plane::resolve` selects it, and an explicit pin
//! with no CLI fails loud at construction. The host box is the sync `Backend`
//! `Result`. No elm-effect program.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use crate::backend::{AgentInfo, PaneInfo, Subscription, WaitOptions, WorkspaceInfo};
use crate::backend::{AgentStatus, BackendError, BackendErrorCode, EventStream, PromptLanding};
use serde_json::{Value, json};
use toron_core::pane_types::TOKEN_PIN;

use crate::backend::rex_cli;

use crate::backend::Backend as MultiplexerBackend;
use rex_cli::rex_map::{
    STALE_WORKING_SECS, board_of, capture_format, child_exit_from_line, newest_record_age_secs,
    parse_process_view, split_direction, state_labels_from_board, status_for_block,
    status_from_program_status, status_with_screen,
};
use rex_cli::{
    Rex, RexBlock, RexEventStream, RexFail, RexSession, RexTarget, block_close_args, capture_args,
    events_args, list_blocks_args, ls_args, new_args, parse_blocks, parse_created_block_id,
    parse_created_session, parse_sessions, parse_target_resolve, process_args, program_status_args,
    read_tail_format, read_whole_format, rename_args, run_args, send_key_args, session_kill_args,
    split_args, split_labelled_args, target_resolve_args, write_args,
};

/// A roster read examines at most this many blocks. Past the cap the remaining
/// blocks are still reported, but with `Unknown` — never a fabricated status.
pub const AGENT_SCAN_CAP: usize = 512;

/// Time one status subprocess may spend. Each block gets this bound
/// independently; there is no shared 3 s sweep deadline, so a slow early
/// block cannot starve later blocks into `Unknown`.
pub const AGENT_STATUS_CALL_BUDGET: Duration = Duration::from_secs(3);

/// Maximum status probes in flight for one roster read. The cap bounds the
/// rex process fan-out; order is restored from the collected results.
pub const AGENT_STATUS_WORKERS: usize = 8;

/// bd-rex-reliability-ioyz D: a session-scoped event stream.
///
/// The trait's `subscribe(&[Subscription])` carries `pane_id` only, so it
/// cannot name a session — `rex events --json` without `-s` follows the
/// CALLER's session, which is the wrong plane for a crew in a session we just
/// created. Measured live: the one-shot `child_exited` never arrived and the
/// outcome degraded to `unknown`. Subscribe by session here instead.
pub fn subscribe_session(
    session: &str,
) -> Result<Box<dyn EventStream>, crate::backend::BackendError> {
    if session.trim().is_empty() {
        return Err(RexFail::Rejected {
            message:
                "rex events needs a session id (an unscoped stream follows the caller's session)"
                    .into(),
        }
        .into_backend());
    }
    let rex = SuperlogicalBackend::rex()?;
    let child = rex
        .spawn(&events_args(Some(session)))
        .map_err(RexFail::into_backend)?;
    let stream = RexEventStream::start(child, Vec::new()).map_err(RexFail::into_backend)?;
    Ok(Box::new(stream))
}

/// Superlogical backend over the `rex` CLI.
pub struct SuperlogicalBackend;

/// bd-rex-reliability-ioyz (A+B): create ONE crew block with the pin label
/// **at creation** and `--keep-open`.
///
/// Two measured defects this closes, both from the create-then-rename window:
/// a live agent block labelled `shell` made `backend_wake_target` answer
/// `no-pane` for a running pin, and a block that closed with its child took
/// the final screen with it (`rex block capture` → "block target not found").
///
/// First block: `rex run` opens a new window. Later blocks ride
/// `create_split_block` anchored to the previous block — `run` has no anchor
/// flag, so a `--split` there would follow focus, not the crew order.
///
/// `command` is the already-rendered launch line (env prefix + program); it is
/// run through `sh -c` because `rex run -- …` takes argv and never parses a
/// shell, while the launch line is produced as one shell line.
pub fn create_agent_block(
    session: &str,
    label: &str,
    cwd: Option<&str>,
    command: &str,
) -> Result<String, crate::backend::BackendError> {
    if command.trim().is_empty() {
        return Err(RexFail::Rejected {
            message: "rex run command is empty".into(),
        }
        .into_backend());
    }
    if label.trim().is_empty() {
        return Err(RexFail::Rejected {
            message: "rex run needs a label (the pin) so the block is routable at creation".into(),
        }
        .into_backend());
    }
    let rex = SuperlogicalBackend::rex()?;
    let argv = ["sh".to_string(), "-c".to_string(), command.to_string()];
    let value = rex
        .json(&run_args(session, label, cwd, &argv))
        .map_err(RexFail::into_backend)?;
    parse_created_block_id(&value).map_err(RexFail::into_backend)
}
/// bd-rex-reliability-ioyz D: parse one `terminal.child_exited` event line
/// into (block_id, exit_code). The event stream keeps the exit as `Raw`
/// (see `rex_map::event_from_line`) so the code survives to the caller.
pub fn child_exit_code(line: &str) -> Option<(String, i32)> {
    child_exit_from_line(line)
}

/// Later crew block: `rex split` anchored to `anchor` with the pin label at
/// creation and `--keep-open`. Same A+B contract as `create_agent_block`;
/// `direction` is already a rex word (`below`, never hc `down`).
pub fn create_split_block(
    session: &str,
    anchor: &str,
    direction: &str,
    label: &str,
    cwd: Option<&str>,
    command: &str,
) -> Result<String, crate::backend::BackendError> {
    if command.trim().is_empty() {
        return Err(RexFail::Rejected {
            message: "rex split command is empty".into(),
        }
        .into_backend());
    }
    if label.trim().is_empty() {
        return Err(RexFail::Rejected {
            message: "rex split needs a label (the pin) so the block is routable at creation"
                .into(),
        }
        .into_backend());
    }
    if anchor.trim().is_empty() {
        return Err(RexFail::Rejected {
            message: "rex split needs the previous block as anchor, never focus".into(),
        }
        .into_backend());
    }
    let rex = SuperlogicalBackend::rex()?;
    let argv = ["sh".to_string(), "-c".to_string(), command.to_string()];
    let value = rex
        .json(&split_labelled_args(
            session, anchor, direction, label, cwd, &argv,
        ))
        .map_err(RexFail::into_backend)?;
    parse_created_block_id(&value).map_err(RexFail::into_backend)
}

impl SuperlogicalBackend {
    fn rex() -> Result<Rex, crate::backend::BackendError> {
        Rex::resolve().map_err(RexFail::into_backend)
    }

    fn locate(rex: &Rex, target: &str) -> Result<(RexSession, RexBlock), RexFail> {
        if target.is_empty() {
            return Err(RexFail::Rejected {
                message: "rex block id is empty".into(),
            });
        }
        // target.resolve searches every session when no caller session is
        // attached. It also accepts suffixes and case-insensitive labels, so
        // verify the exact id or label before accepting: identity semantics
        // stay stricter than the resolver's convenience matching.
        let hit = parse_target_resolve(&rex.json(&target_resolve_args(target))?, target)?;
        if !exact_target(&hit, target) {
            return Err(RexFail::Rejected {
                message: format!("rex block {target} not found"),
            });
        }
        let session = RexSession {
            session_id: hit.session_id.clone(),
            label: String::new(),
            pane_count: 0,
            tab_count: 0,
            focused: false,
            active_tab_id: String::new(),
            cwd: None,
        };
        // Resolve once, then fetch that session's blocks for creator/window
        // metadata. This is two calls instead of one list_blocks per session.
        let block = Self::blocks(rex, &hit.session_id)?
            .into_iter()
            .find(|block| block.block_id == hit.block_id)
            .unwrap_or_else(|| RexBlock {
                block_id: hit.block_id,
                label: hit.label,
                creator_name: String::new(),
                window_id: hit.window_id,
            });
        Ok((session, block))
    }

    fn blocks(rex: &Rex, session: &str) -> Result<Vec<RexBlock>, RexFail> {
        parse_blocks(&rex.json(&list_blocks_args(session))?)
    }

    fn block_state(
        rex: &Rex,
        session: &str,
        block: &str,
    ) -> Result<(AgentStatus, serde_json::Map<String, Value>), RexFail> {
        let records = rex.json(&program_status_args(session, block))?;
        let (board, order) = board_of(&records);
        let labels = state_labels_from_board(&board);
        let status = if board.is_empty() {
            let process = rex.json(&process_args(session, block))?;
            status_for_block(&board, &order, &parse_process_view(&process))
        } else {
            status_from_program_status(&records)
        };
        // bd-0uzrq.10: a `working` record can be a lie (opencode does not emit
        // `idle` after a turn that ran a background shell). Only when the fold
        // says working AND the record has been silent past the threshold do we
        // pay for a screen capture, and only the screen can overrule it. This
        // is the ONE detector: every consumer reaches it through here.
        if status == AgentStatus::Working {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            let age = newest_record_age_secs(&records, now);
            if age.is_some_and(|secs| secs >= STALE_WORKING_SECS)
                && let Ok(out) = rex.run(&capture_args(session, block, "text"))
            {
                let screen = String::from_utf8_lossy(&out.stdout);
                return Ok((status_with_screen(status, age, &screen), labels));
            }
        }
        Ok((status, labels))
    }

    fn block_status(rex: &Rex, session: &str, block: &str) -> Result<AgentStatus, RexFail> {
        Self::block_state(rex, session, block).map(|(status, _)| status)
    }

    /// C: readiness gate with an explicit bound (production: 10 s).
    /// Tests drive this with milliseconds so the refusal path stays fast.
    fn prompt_gated(
        &self,
        target: &str,
        text: &str,
        wait: Duration,
    ) -> Result<Value, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, target).map_err(RexFail::into_backend)?;
        let ready = |s: &AgentStatus| {
            matches!(
                s,
                AgentStatus::Idle | AgentStatus::Done | AgentStatus::Blocked
            )
        };
        let mut status = Self::block_status(&rex, &session.session_id, &block.block_id)
            .map_err(RexFail::into_backend)?;
        if !ready(&status) {
            let deadline = std::time::Instant::now() + wait;
            while std::time::Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(50));
                status = Self::block_status(&rex, &session.session_id, &block.block_id)
                    .map_err(RexFail::into_backend)?;
                if ready(&status) {
                    break;
                }
            }
        }
        if !ready(&status) {
            return Err(BackendError::new(
                BackendErrorCode::Other("busy".into()),
                format!(
                    "rex prompt refused: block {} status {} is not at a prompt",
                    block.block_id,
                    status.as_str(),
                ),
            ));
        }
        self.prompt_ready(&session.session_id, &block.block_id, text)
    }

    /// Write + Enter once the gate has proven the pane is at a prompt.
    fn prompt_ready(
        &self,
        session: &str,
        block: &str,
        text: &str,
    ) -> Result<Value, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let receipt = rex
            .json(&write_args(session, block, text))
            .map_err(RexFail::into_backend)?;
        rex.text(&send_key_args(session, block, &["enter"]))
            .map_err(RexFail::into_backend)?;
        Ok(receipt)
    }
}

impl MultiplexerBackend for SuperlogicalBackend {
    fn name(&self) -> &'static str {
        "superlogical"
    }

    fn workspace_list(&self) -> Result<Vec<WorkspaceInfo>, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let sessions = parse_sessions(&rex.json(&ls_args()).map_err(RexFail::into_backend)?)
            .map_err(RexFail::into_backend)?;
        Ok(sessions
            .into_iter()
            .enumerate()
            .map(|(index, session)| workspace_from(session, index))
            .collect())
    }

    fn workspace_create(
        &self,
        label: Option<&str>,
    ) -> Result<WorkspaceInfo, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let value = rex.json(&new_args(label)).map_err(RexFail::into_backend)?;
        let session = parse_created_session(&value, label).map_err(RexFail::into_backend)?;
        Ok(workspace_from(session, 0))
    }

    fn pane_list(
        &self,
        workspace_id: Option<&str>,
    ) -> Result<Vec<PaneInfo>, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let sessions = match workspace_id {
            Some(id) => vec![id.to_string()],
            None => parse_sessions(&rex.json(&ls_args()).map_err(RexFail::into_backend)?)
                .map_err(RexFail::into_backend)?
                .into_iter()
                .map(|session| session.session_id)
                .collect(),
        };
        let mut panes = Vec::new();
        for session in sessions {
            let blocks = Self::blocks(&rex, &session).map_err(RexFail::into_backend)?;
            for block in blocks {
                panes.push(pane_from(&block, &session, AgentStatus::Unknown));
            }
        }
        Ok(panes)
    }

    fn pane_split(
        &self,
        parent_pane_id: &str,
        direction: &str,
        cwd: Option<&str>,
    ) -> Result<PaneInfo, crate::backend::BackendError> {
        let Some(direction) = split_direction(direction) else {
            return Err(RexFail::Rejected {
                message: format!(
                    "rex split direction {direction:?} is not left, right, above, or below"
                ),
            }
            .into_backend());
        };
        let rex = Self::rex()?;
        let (session, parent) =
            Self::locate(&rex, parent_pane_id).map_err(RexFail::into_backend)?;
        let value = rex
            .json(&split_args(
                &session.session_id,
                &parent.block_id,
                direction,
                cwd,
            ))
            .map_err(RexFail::into_backend)?;
        let id = parse_created_block_id(&value).map_err(RexFail::into_backend)?;
        let window = value
            .get("window_id")
            .and_then(Value::as_str)
            .unwrap_or(parent.window_id.as_str());
        Ok(pane_from(
            &RexBlock {
                block_id: id,
                label: String::new(),
                creator_name: String::new(),
                window_id: window.to_string(),
            },
            &session.session_id,
            AgentStatus::Unknown,
        ))
    }

    fn pane_close(&self, pane_id: &str) -> Result<(), crate::backend::BackendError> {
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, pane_id).map_err(RexFail::into_backend)?;
        rex.text(&block_close_args(&session.session_id, &block.block_id))
            .map_err(RexFail::into_backend)?;
        Ok(())
    }

    fn pane_run(&self, pane_id: &str, command: &str) -> Result<(), crate::backend::BackendError> {
        if command.is_empty() {
            return Err(RexFail::Rejected {
                message: "rex run command is empty".into(),
            }
            .into_backend());
        }
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, pane_id).map_err(RexFail::into_backend)?;
        rex.json(&write_args(&session.session_id, &block.block_id, command))
            .map_err(RexFail::into_backend)?;
        rex.text(&send_key_args(
            &session.session_id,
            &block.block_id,
            &["enter"],
        ))
        .map_err(RexFail::into_backend)?;
        Ok(())
    }

    fn agent_list(&self) -> Result<Vec<AgentInfo>, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let sessions = parse_sessions(&rex.json(&ls_args()).map_err(RexFail::into_backend)?)
            .map_err(RexFail::into_backend)?;

        // One roster row per block, in session/list order. No status probe is
        // started before the whole roster is known, so results never depend
        // on which earlier block happened to be slow.
        let mut targets: Vec<(String, RexBlock)> = Vec::new();
        for session in sessions {
            let blocks = Self::blocks(&rex, &session.session_id).map_err(RexFail::into_backend)?;
            for block in blocks {
                targets.push((session.session_id.clone(), block));
            }
        }
        let mut agents: Vec<AgentInfo> = targets
            .iter()
            .map(|(session, block)| {
                agent_from(block, session, AgentStatus::Unknown, serde_json::Map::new())
            })
            .collect();

        let status_count = status_scan_count(targets.len());
        if status_count == 0 {
            return Ok(agents);
        }

        // Status is one `program_status` subprocess per block (plus a process
        // probe when no record exists). Probe a bounded number concurrently;
        // each call gets its own wait instead of a shared wall-clock deadline.
        let status_rex = rex.with_wait(AGENT_STATUS_CALL_BUDGET);
        let next = std::sync::atomic::AtomicUsize::new(0);
        let results: Vec<std::sync::Mutex<Option<Result<_, RexFail>>>> = (0..status_count)
            .map(|_| std::sync::Mutex::new(None))
            .collect();
        std::thread::scope(|scope| {
            for _ in 0..status_workers(targets.len()) {
                scope.spawn(|| {
                    loop {
                        let index = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if index >= status_count {
                            break;
                        }
                        let (session, block) = &targets[index];
                        let result = Self::block_state(&status_rex, session, &block.block_id);
                        if let Ok(mut slot) = results[index].lock() {
                            *slot = Some(result);
                        }
                    }
                });
            }
        });

        for (index, result) in results.into_iter().enumerate() {
            let result = result
                .into_inner()
                .map_err(|_| {
                    crate::backend::BackendError::new(
                        crate::backend::BackendErrorCode::Other("rex_status_worker".into()),
                        "rex status worker panicked",
                    )
                })?
                .ok_or_else(|| {
                    crate::backend::BackendError::new(
                        crate::backend::BackendErrorCode::Other("rex_status_worker".into()),
                        "rex status worker returned no result",
                    )
                })?
                .map_err(RexFail::into_backend)?;
            let (session, block) = &targets[index];
            let (status, labels) = result;
            agents[index] = agent_from(block, session, status, labels);
        }
        Ok(agents)
    }

    fn prompt(
        &self,
        target: &str,
        text: &str,
        _wait: Option<&WaitOptions>,
    ) -> Result<Value, crate::backend::BackendError> {
        self.prompt_gated(target, text, Duration::from_secs(10))
    }

    fn agent_read(
        &self,
        target: &str,
        source: &str,
        lines: u32,
    ) -> Result<Value, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, target).map_err(RexFail::into_backend)?;
        let format = capture_format(source);
        let text = if lines == 0 {
            read_whole_format(&rex, &session.session_id, &block.block_id, format)
        } else {
            read_tail_format(&rex, &session.session_id, &block.block_id, format, lines)
        }
        .map_err(RexFail::into_backend)?;
        Ok(json!({ "text": text }))
    }

    fn workspace_close(
        &self,
        workspace_id: &str,
        close_group: bool,
    ) -> Result<(), crate::backend::BackendError> {
        // rex has ONE close primitive and it always closes the whole session.
        // Refuse a narrower request instead of silently killing more than the
        // caller asked for (the fallback treats Unsupported as no-replay).
        workspace_close_scope(close_group)?;
        let rex = Self::rex()?;
        rex.text(&session_kill_args(workspace_id))
            .map_err(RexFail::into_backend)?;
        Ok(())
    }

    fn agent_rename(
        &self,
        target: &str,
        new_name: Option<&str>,
    ) -> Result<(), crate::backend::BackendError> {
        let Some(label) = new_name.filter(|name| !name.is_empty()) else {
            return Err(RexFail::Rejected {
                message: "rex rename needs a label".into(),
            }
            .into_backend());
        };
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, target).map_err(RexFail::into_backend)?;
        rex.text(&rename_args(&session.session_id, &block.block_id, label))
            .map_err(RexFail::into_backend)?;
        Ok(())
    }

    fn agent_send_keys(
        &self,
        target: &str,
        keys: &[&str],
    ) -> Result<(), crate::backend::BackendError> {
        if keys.is_empty() {
            return Ok(());
        }
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, target).map_err(RexFail::into_backend)?;
        rex.text(&send_key_args(&session.session_id, &block.block_id, keys))
            .map_err(RexFail::into_backend)?;
        Ok(())
    }

    fn agent_start(
        &self,
        name: &str,
        kind: &str,
        pane_id: &str,
        args: &[String],
    ) -> Result<AgentInfo, crate::backend::BackendError> {
        let mut command = kind.to_string();
        for arg in args {
            command.push(' ');
            command.push_str(arg);
        }
        self.pane_run(pane_id, &command)?;
        if !name.is_empty() {
            self.agent_rename(pane_id, Some(name))?;
        }
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, pane_id).map_err(RexFail::into_backend)?;
        let (status, labels) = Self::block_state(&rex, &session.session_id, &block.block_id)
            .unwrap_or((AgentStatus::Unknown, serde_json::Map::new()));
        let mut agent = agent_from(&block, &session.session_id, status, labels);
        if !name.is_empty() {
            agent.name = Some(name.to_string());
        }
        Ok(agent)
    }

    fn agent_wait(
        &self,
        target: &str,
        until: &[AgentStatus],
        timeout_ms: Option<u64>,
    ) -> Result<AgentInfo, crate::backend::BackendError> {
        let deadline = timeout_ms.map(|ms| Instant::now() + Duration::from_millis(ms));
        loop {
            let rex = Self::rex()?;
            let (session, block) = Self::locate(&rex, target).map_err(RexFail::into_backend)?;
            let (status, labels) = Self::block_state(&rex, &session.session_id, &block.block_id)
                .map_err(RexFail::into_backend)?;
            if until.is_empty() || until.contains(&status) {
                return Ok(agent_from(&block, &session.session_id, status, labels));
            }
            match deadline {
                None => {
                    return Err(BackendError::timeout(
                        "rex status did not match and no timeout was set",
                    ));
                }
                Some(deadline) if Instant::now() >= deadline => {
                    return Err(BackendError::timeout(format!(
                        "rex status {status:?} did not become {until:?}"
                    )));
                }
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    std::thread::sleep(Duration::from_millis(20).min(left));
                }
            }
        }
    }

    fn prompt_coded(
        &self,
        target: &str,
        text: &str,
        wait: Option<&WaitOptions>,
    ) -> Result<PromptLanding, crate::backend::BackendError> {
        let rex = Self::rex()?;
        let (session, block) = Self::locate(&rex, target).map_err(RexFail::into_backend)?;
        let status = Self::block_status(&rex, &session.session_id, &block.block_id)
            .map_err(RexFail::into_backend)?;
        if status == AgentStatus::Blocked {
            return Ok(PromptLanding {
                status: None,
                code: Some("agent_blocked".into()),
            });
        }
        self.prompt(target, text, wait)?;
        if let Some(wait) = wait {
            let agent = self.agent_wait(target, &wait.until, wait.timeout_ms)?;
            return Ok(PromptLanding {
                status: Some(agent.agent_status),
                code: None,
            });
        }
        Ok(PromptLanding {
            status: Some(status),
            code: None,
        })
    }

    fn pane_wait_output(
        &self,
        pane_id: &str,
        needle: &str,
        timeout_ms: u64,
    ) -> Result<Value, crate::backend::BackendError> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            let value = self.agent_read(pane_id, "text", 0)?;
            let text = value.get("text").and_then(Value::as_str).unwrap_or("");
            if text.contains(needle) {
                return Ok(value);
            }
            if Instant::now() >= deadline {
                return Err(BackendError::timeout(format!(
                    "rex output did not contain {needle:?}"
                )));
            }
            let left = deadline.saturating_duration_since(Instant::now());
            std::thread::sleep(Duration::from_millis(20).min(left));
        }
    }

    fn subscribe(
        &self,
        filters: &[Subscription],
    ) -> Result<Box<dyn EventStream>, crate::backend::BackendError> {
        let rex = Self::rex()?;
        // `rex events` without `-s` follows the CALLER's session, which is the
        // wrong plane for a crew. Resolve the session from the panes instead,
        // and refuse a cross-session set rather than silently following one.
        let mut sessions = BTreeSet::new();
        let mut panes = Vec::new();
        for filter in filters {
            if filter.pane_id.is_empty() {
                continue;
            }
            let (session, block) =
                Self::locate(&rex, &filter.pane_id).map_err(RexFail::into_backend)?;
            sessions.insert(session.session_id);
            panes.push(block.block_id);
        }
        let session = single_session(&sessions).map_err(RexFail::into_backend)?;
        let child = rex
            .spawn(&events_args(Some(&session)))
            .map_err(RexFail::into_backend)?;
        let stream = RexEventStream::start(child, panes).map_err(RexFail::into_backend)?;
        Ok(Box::new(stream))
    }
}

/// The one session a pane set names. Pure so the refusal is testable without a
/// rex server: the trait carries no session, and guessing is how a crew
/// subscribed to the caller's session instead of its own.
fn single_session(sessions: &BTreeSet<String>) -> Result<String, RexFail> {
    match sessions.len() {
        0 => Err(RexFail::Rejected {
            message: "rex subscribe needs at least one pane id (a session-less stream \
                      follows the caller's session)"
                .into(),
        }),
        1 => Ok(sessions
            .iter()
            .next()
            .cloned()
            .expect("length checked above")),
        n => Err(RexFail::Rejected {
            message: format!(
                "rex subscribe: the panes span {n} sessions; subscribe one session at a time \
                 (use subscribe_session for an explicit crew session)"
            ),
        }),
    }
}

/// rex's `session kill` closes every block in the session. A caller asking for
/// a narrower scope is refused, never widened silently.
fn workspace_close_scope(close_group: bool) -> Result<(), BackendError> {
    if close_group {
        return Ok(());
    }
    Err(BackendError::new(
        BackendErrorCode::Unsupported,
        "rex workspace_close has one scope: session kill closes every block in the session; \
         close_group=false is refused rather than killing more than asked",
    ))
}

/// `target.resolve` also accepts suffixes and case-insensitive labels. The
/// adapter keeps the older, stricter identity contract: a query must equal the
/// block id or its label exactly.
fn exact_target(hit: &RexTarget, target: &str) -> bool {
    hit.block_id == target || hit.label == target
}

fn workspace_from(session: RexSession, index: usize) -> WorkspaceInfo {
    WorkspaceInfo {
        workspace_id: session.session_id,
        number: index as u64 + 1,
        label: session.label,
        focused: session.focused,
        pane_count: session.pane_count,
        tab_count: session.tab_count,
        active_tab_id: session.active_tab_id,
        agent_status: AgentStatus::Unknown,
        cwd: session.cwd,
    }
}

fn pane_from(block: &RexBlock, session: &str, status: AgentStatus) -> PaneInfo {
    // ADR-0045 §6: a rex block is labelled with the pin at creation. Map it
    // onto the canonical mail-identity token HERE, at the adapter edge, so the
    // pure identity cores (`pane_token_pin`, `pin_pane_map`) need no rex
    // special case. `toron_npub`/`toron_project` stay absent: a block label is
    // not a full stamp, so `pane_stamp` declines it and divergence checks skip.
    let mut tokens = serde_json::Map::new();
    if !block.label.is_empty() {
        tokens.insert(TOKEN_PIN.to_string(), Value::String(block.label.clone()));
    }
    PaneInfo {
        pane_id: block.block_id.clone(),
        terminal_id: String::new(),
        workspace_id: session.to_string(),
        tab_id: block.window_id.clone(),
        focused: false,
        agent_status: status,
        revision: 0,
        title: none_if_empty(&block.label),
        cwd: None,
        agent: none_if_empty(&block.creator_name),
        agent_session: None,
        display_agent: none_if_empty(&block.creator_name),
        foreground_cwd: None,
        label: none_if_empty(&block.label),
        scroll: None,
        state_labels: serde_json::Map::new(),
        terminal_title: none_if_empty(&block.label),
        terminal_title_stripped: none_if_empty(&block.label),
        tokens,
    }
}

fn agent_from(
    block: &RexBlock,
    session: &str,
    status: AgentStatus,
    state_labels: serde_json::Map<String, Value>,
) -> AgentInfo {
    AgentInfo {
        agent: none_if_empty(&block.creator_name),
        agent_status: status,
        cwd: None,
        display_agent: none_if_empty(&block.label).or_else(|| none_if_empty(&block.creator_name)),
        focused: false,
        foreground_cwd: None,
        interactive_ready: status == AgentStatus::Idle,
        launch_pending: false,
        name: none_if_empty(&block.label),
        pane_id: block.block_id.clone(),
        revision: 0,
        state_change_seq: 0,
        completion_seq: None,
        state_labels,
        tab_id: block.window_id.clone(),
        terminal_id: String::new(),
        terminal_title: none_if_empty(&block.label),
        workspace_id: session.to_string(),
    }
}

/// How many blocks receive a status probe. The remainder stay `Unknown` but
/// are still listed, so the cap never hides a pane from reconciliation.
fn status_scan_count(blocks: usize) -> usize {
    blocks.min(AGENT_SCAN_CAP)
}

/// How many status probes run at once for a roster of `blocks`.
fn status_workers(blocks: usize) -> usize {
    AGENT_STATUS_WORKERS.min(status_scan_count(blocks))
}

fn none_if_empty(text: &str) -> Option<String> {
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

#[cfg(test)]
mod pane_identity_tests {
    use super::*;

    fn block(label: &str) -> RexBlock {
        RexBlock {
            block_id: "b7".to_string(),
            label: label.to_string(),
            creator_name: "rex".to_string(),
            window_id: "w1".to_string(),
        }
    }

    /// ADR-0045 §6: the adapter maps a rex block label onto `toron_pin`, so
    /// the pure identity cores resolve a rex pane with no rex special case.
    /// Mutation: drop the token mapping and this fails while `pane.label`
    /// still carries the pin — the exact silent gap the mapping closes.
    #[test]
    fn a_labelled_block_carries_the_canonical_pin_token() {
        let pane = pane_from(&block("DustySparrow"), "s1", AgentStatus::Idle);
        assert_eq!(
            toron_core::mailbox::identity::pane_token_pin(&pane),
            Some("DustySparrow")
        );
    }

    /// An unlabelled split block carries no pin: an empty label must never
    /// become an empty identity.
    #[test]
    fn an_unlabelled_block_carries_no_pin_token() {
        let pane = pane_from(&block(""), "s1", AgentStatus::Unknown);
        assert_eq!(toron_core::mailbox::identity::pane_token_pin(&pane), None);
    }

    /// A rex label is not a full stamp (no npub), so the divergence check
    /// declines it instead of comparing a half-stamp.
    #[test]
    fn a_block_label_is_not_a_full_stamp() {
        let pane = pane_from(&block("DustySparrow"), "s1", AgentStatus::Idle);
        assert_eq!(toron_core::mailbox::identity::pane_stamp(&pane), None);
    }

    /// `rex events` follows the caller's session unless `-s` is passed, so a
    /// pane set must name exactly one session. Mutation: accept a multi-session
    /// set and the subscription silently follows whichever session rex picks.
    #[test]
    fn a_pane_set_must_name_exactly_one_session() {
        assert!(
            single_session(&BTreeSet::new()).is_err(),
            "empty is not all"
        );
        let one: BTreeSet<String> = ["s1".to_string()].into_iter().collect();
        assert_eq!(single_session(&one).expect("one session"), "s1");
        let two: BTreeSet<String> = ["s1".to_string(), "s2".to_string()].into_iter().collect();
        let err = single_session(&two).expect_err("two sessions must refuse");
        match err {
            RexFail::Rejected { message } => assert!(
                message.contains("subscribe one session at a time"),
                "{message}"
            ),
            _ => panic!("a cross-session set must be a typed refusal, not a transport error"),
        }
    }

    /// `session kill` closes the whole session; a narrower request must refuse
    /// instead of widening to it.
    #[test]
    fn a_narrow_workspace_close_is_refused_not_widened() {
        assert!(workspace_close_scope(true).is_ok());
        let err = workspace_close_scope(false).expect_err("close_group=false must refuse");
        assert!(err.to_string().contains("close_group=false"), "{err}");
    }

    /// Live probe (opt-in). Creates a throwaway rex session, runs one labelled
    /// `sleep` block, and proves the label→pin mapping, the roster entry, and
    /// that cleanup removed the session. Run with:
    /// `mbx test -p toron-core --lib live_rex -- --ignored --nocapture`.
    #[test]
    #[ignore = "live rex: creates and kills a throwaway session"]
    fn live_rex_round_trip_labels_a_block_and_resolves_identity() {
        struct LiveSession(String);
        impl Drop for LiveSession {
            fn drop(&mut self) {
                let _ = SuperlogicalBackend.workspace_close(&self.0, true);
            }
        }

        let backend = SuperlogicalBackend;
        let label = format!("toron-live-{}", std::process::id());
        let workspace = backend
            .workspace_create(Some(&label))
            .expect("create a throwaway rex session");
        let guard = LiveSession(workspace.workspace_id.clone());

        let pin = "LiveProbePin";
        let block_id = create_agent_block(&workspace.workspace_id, pin, None, "sleep 120")
            .expect("run one labelled keep-open block");
        assert!(!block_id.is_empty());

        let panes = backend
            .pane_list(Some(&workspace.workspace_id))
            .expect("pane list");
        let pane = panes
            .iter()
            .find(|pane| pane.pane_id == block_id)
            .expect("the new block is in pane_list");
        assert_eq!(
            toron_core::mailbox::identity::pane_token_pin(pane),
            Some(pin),
            "the block label is the canonical pin token"
        );
        assert!(
            toron_core::mailbox::identity::pane_stamp(pane).is_none(),
            "a label alone is not a full stamp"
        );

        let agents = backend.agent_list().expect("agent list");
        let agent = agents
            .iter()
            .find(|agent| agent.pane_id == block_id)
            .expect("the new block is in agent_list");
        assert_eq!(agent.name.as_deref(), Some(pin));

        let session = workspace.workspace_id.clone();
        drop(guard);
        let remaining = backend.workspace_list().expect("list after close");
        assert!(
            !remaining
                .iter()
                .any(|workspace| workspace.workspace_id == session),
            "cleanup left the throwaway session behind"
        );
    }

    /// `target.resolve` is deliberately more permissive than the adapter's
    /// identity contract; this pins the boundary.
    #[test]
    fn target_resolution_requires_an_exact_id_or_label() {
        let hit = RexTarget {
            session_id: "s1".into(),
            block_id: "block:abc".into(),
            window_id: "w1".into(),
            label: "DustySparrow".into(),
        };
        assert!(exact_target(&hit, "block:abc"));
        assert!(exact_target(&hit, "DustySparrow"));
        assert!(!exact_target(&hit, "dusterysparrow"));
        assert!(!exact_target(&hit, "abc"));
    }

    /// Live probe (opt-in): format, tail-over-blob, and label lookup through
    /// target.resolve. Run with:
    /// `mbx test -p toron-plane live_rex_format -- --ignored --nocapture`.
    #[test]
    #[ignore = "live rex: creates and kills a throwaway session"]
    fn live_rex_format_blob_and_target_resolve_round_trip() {
        struct LiveSession(String);
        impl Drop for LiveSession {
            fn drop(&mut self) {
                let _ = SuperlogicalBackend.workspace_close(&self.0, true);
            }
        }

        let backend = SuperlogicalBackend;
        let label = format!("toron-format-{}", std::process::id());
        let workspace = backend
            .workspace_create(Some(&label))
            .expect("create a throwaway rex session");
        let _guard = LiveSession(workspace.workspace_id.clone());
        let pin = "FormatLivePin";
        let block = create_agent_block(&workspace.workspace_id, pin, None, "seq 1 20000")
            .expect("run a labelled block");

        backend
            .pane_wait_output(&block, "20000", 15_000)
            .expect("the generated output must land in the pane");

        let whole = backend
            .agent_read(&block, "text", 0)
            .expect("whole read through format");
        let whole = whole["text"].as_str().expect("text field");
        assert!(whole.starts_with("1\n"), "whole read starts at the top");
        assert!(whole.contains("20000"), "whole read reaches the bottom");

        let tail = backend
            .agent_read(&block, "text", 3)
            .expect("tail read through format + blob_read");
        let tail = tail["text"].as_str().expect("text field");
        assert!(
            tail.contains("19999"),
            "tail keeps the last lines: {tail:?}"
        );
        assert!(
            tail.contains("20000"),
            "tail keeps the final line: {tail:?}"
        );
        assert!(
            !tail.contains("10000"),
            "tail must not fetch the top: {tail:?}"
        );

        let by_label = backend
            .agent_read(pin, "text", 0)
            .expect("label lookup through target.resolve");
        assert!(
            by_label["text"]
                .as_str()
                .is_some_and(|text| text.contains("20000")),
            "label lookup reached the same block"
        );
    }

    /// `agent_list` used to spend one shared 3 s deadline across the sweep;
    /// once it expired, every later block silently became `Unknown`. The
    /// replacement bounds each status call and caps how many blocks are
    /// enriched, while still listing the rest.
    #[test]
    fn the_roster_sweep_caps_status_work_without_a_shared_deadline() {
        assert_eq!(status_scan_count(0), 0);
        assert_eq!(status_scan_count(3), 3);
        assert_eq!(status_scan_count(AGENT_SCAN_CAP + 17), AGENT_SCAN_CAP);
        assert_eq!(status_workers(0), 0);
        assert_eq!(status_workers(1), 1);
        assert_eq!(status_workers(AGENT_SCAN_CAP + 1), AGENT_STATUS_WORKERS);
    }
}
