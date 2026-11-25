// governed-by: ADR-0031
//! Pure Swarm Board reducer: `tick(&Model, Msg) -> (Model, Vec<Effect>)`.
//!
//! No I/O, no clock reads, no randomness. Posts, roster, leases, and receipts
//! refresh on `Init`, `BusChanged`, and manual `Refresh` — never polled on
//! ticks. `:18080` health probes run every `10s` single-flight; `flywheel
//! snapshot` runs every `30s` single-flight; both pause when unfocused or idle
//! for 5 minutes and resume on the next keypress.

use super::model::{
    FLYWHEEL_PROBE_INTERVAL_SECS, IDLE_PAUSE_SECS, Inspector, Mode, Model, StatusKind,
    TORON_PROBE_INTERVAL_SECS,
};
use super::msg::{Effect, Msg, Op};

/// Replay a sequence of messages from `seed`, collecting all emitted effects.
#[cfg(test)]
pub fn replay(seed: &Model, log: &[Msg]) -> (Model, Vec<Effect>) {
    let mut model = seed.clone();
    let mut all_effects = Vec::new();
    for msg in log {
        let (next, effects) = tick(&model, msg.clone());
        model = next;
        all_effects.extend(effects);
    }
    (model, all_effects)
}

/// Pure reducer step. Terminal state (`should_quit`) emits zero further effects.
pub fn tick(model: &Model, msg: Msg) -> (Model, Vec<Effect>) {
    let mut m = model.clone();
    if m.should_quit {
        return (m, Vec::new());
    }
    let mut effects = Vec::new();

    if is_user_input(&msg) {
        m.last_input_unix = m.now_unix;
    }

    match msg {
        Msg::Init => {
            effects.extend(event_driven_loads());
            m.toron_probe_inflight = true;
            m.flywheel_probe_inflight = true;
            effects.push(Effect::ProbeToronHealth);
            effects.push(Effect::ProbeFlywheelSnapshot);
        }
        Msg::BusChanged => {
            effects.extend(event_driven_loads());
        }
        Msg::Tick { now_unix } => {
            m.now_unix = now_unix;
            m.tick = m.tick.wrapping_add(1);
            if m.last_input_unix == 0 {
                m.last_input_unix = now_unix;
            }
            if !m.focused || now_unix.saturating_sub(m.last_input_unix) >= IDLE_PAUSE_SECS {
                return (m, effects);
            }
            if !m.toron_probe_inflight
                && m.last_toron_probe_unix
                    .is_none_or(|t| now_unix.saturating_sub(t) >= TORON_PROBE_INTERVAL_SECS)
            {
                m.toron_probe_inflight = true;
                effects.push(Effect::ProbeToronHealth);
            }
            if !m.flywheel_probe_inflight
                && m.last_flywheel_probe_unix
                    .is_none_or(|t| now_unix.saturating_sub(t) >= FLYWHEEL_PROBE_INTERVAL_SECS)
            {
                m.flywheel_probe_inflight = true;
                effects.push(Effect::ProbeFlywheelSnapshot);
            }
        }
        Msg::FocusChanged(focused) => {
            m.focused = focused;
            if focused {
                m.last_input_unix = m.now_unix;
            }
        }
        Msg::BoardLoaded(posts) => {
            m.posts = posts;
            if m.follow {
                let len = m.visible_posts().len();
                m.selected_post = len.saturating_sub(1);
                m.msg_scroll = 0;
            }
            m.clamp_selection();
        }
        Msg::RosterLoaded(roster) => m.roster = roster,
        Msg::LeasesLoaded(leases) => m.leases = leases,
        Msg::ReceiptsLoaded(receipts) => m.receipts = receipts,
        Msg::SealedCountsLoaded(counts) => m.sealed_inbox = counts,
        Msg::ToronHealthLoaded { signals, now_unix } => {
            m.toron_probe_inflight = false;
            m.last_toron_probe_unix = Some(now_unix);
            m.toron_health = signals;
            m.clamp_selection();
        }
        Msg::FlywheelSnapshotLoaded { panel, now_unix } => {
            m.flywheel_probe_inflight = false;
            m.last_flywheel_probe_unix = Some(now_unix);
            m.flywheel = panel;
            m.clamp_selection();
        }
        Msg::BoardPosted(post) => {
            m.compose_draft = None;
            m.mode = Mode::Normal;
            let seq = post.seq;
            m.posts.push(post);
            m.follow = true;
            m.selected_post = m.visible_posts().len().saturating_sub(1);
            m.msg_scroll = 0;
            m.set_status(StatusKind::Ok, format!("posted #{seq}"));
        }
        Msg::Inflight(n) => m.inflight = n,
        Msg::Failed { op, message } => {
            match op {
                Op::Health => m.toron_probe_inflight = false,
                Op::Flywheel => m.flywheel_probe_inflight = false,
                _ => {}
            }
            m.set_status(StatusKind::Error, format!("{}: {message}", op.label()));
        }

        Msg::Quit => {
            m.should_quit = true;
            effects.push(Effect::Quit);
        }
        Msg::ToggleHelp => {
            m.mode = if m.mode == Mode::Help {
                Mode::Normal
            } else {
                Mode::Help
            };
        }
        Msg::SelectInspector(inspector) => {
            m.inspector = inspector;
            m.clamp_selection();
        }
        Msg::CycleInspector(delta) => {
            m.inspector = m.inspector.step(delta);
            m.clamp_selection();
        }
        Msg::MovePost(delta) => {
            let len = m.visible_posts().len();
            if len > 0 {
                let next = (m.selected_post as isize + delta).clamp(0, len as isize - 1) as usize;
                if next != m.selected_post {
                    m.selected_post = next;
                    m.msg_scroll = 0;
                }
                if delta < 0 {
                    m.follow = false;
                } else if next + 1 == len {
                    m.follow = true;
                }
            }
        }
        Msg::GotoPost(top) => {
            let len = m.visible_posts().len();
            if len > 0 {
                m.selected_post = if top { 0 } else { len - 1 };
                m.msg_scroll = 0;
                m.follow = !top;
            }
        }
        Msg::ScrollMessage(delta) => {
            m.msg_scroll = (m.msg_scroll as isize + delta).max(0) as usize;
        }
        Msg::ToggleFollow => {
            m.follow = !m.follow;
            if m.follow {
                let len = m.visible_posts().len();
                m.selected_post = len.saturating_sub(1);
                m.msg_scroll = 0;
            }
            m.set_status(
                StatusKind::Info,
                if m.follow {
                    "follow: on"
                } else {
                    "follow: off"
                },
            );
        }
        Msg::ToggleRetired => {
            m.show_retired = !m.show_retired;
            m.set_status(
                StatusKind::Info,
                if m.show_retired {
                    "retired seats: shown"
                } else {
                    "retired seats: collapsed"
                },
            );
        }
        Msg::MoveDiag(delta) => {
            let len = m.diagnostics().len();
            if len > 0 {
                m.selected_diag =
                    (m.selected_diag as isize + delta).clamp(0, len as isize - 1) as usize;
            }
        }
        Msg::CopyRemedy => {
            let diags = m.diagnostics();
            if let Some(d) = diags.get(m.selected_diag) {
                m.set_status(StatusKind::Info, format!("remedy: {}", d.remedy_cmd));
            } else {
                m.set_status(StatusKind::Info, "no active diagnostic");
            }
        }
        Msg::OpenFilter => m.mode = Mode::Filter,
        Msg::CancelFilter => {
            m.mode = Mode::Normal;
            m.filter.clear();
            m.clamp_selection();
        }
        Msg::FilterChar(c) => {
            if m.mode == Mode::Filter {
                m.filter.push(c);
                m.clamp_selection();
            }
        }
        Msg::FilterBackspace => {
            if m.mode == Mode::Filter {
                m.filter.pop();
                m.clamp_selection();
            }
        }
        Msg::CommitFilter => m.mode = Mode::Normal,
        Msg::Refresh => {
            m.set_status(StatusKind::Info, "refreshing");
            effects.extend(event_driven_loads());
            if !m.toron_probe_inflight {
                m.toron_probe_inflight = true;
                effects.push(Effect::ProbeToronHealth);
            }
            if !m.flywheel_probe_inflight {
                m.flywheel_probe_inflight = true;
                effects.push(Effect::ProbeFlywheelSnapshot);
            }
        }
        Msg::StepRefresh(step) => {
            m.settings.refresh_ms = m.settings.step_refresh(step);
            let ms = m.settings.refresh_ms;
            m.set_status(StatusKind::Info, format!("tick: {ms}ms"));
        }
        Msg::ToggleRelativeTime => {
            m.settings.relative_time = !m.settings.relative_time;
            m.set_status(
                StatusKind::Info,
                if m.settings.relative_time {
                    "time: relative"
                } else {
                    "time: clock"
                },
            );
        }

        Msg::OpenCompose => {
            if m.settings.agent.is_none() {
                m.set_status(
                    StatusKind::Error,
                    "pass --as <pin> or export AGENT_NAME to post",
                );
            } else {
                m.mode = Mode::Compose;
                m.compose_draft.get_or_insert_with(String::new);
            }
        }
        Msg::ComposeChar(c) => {
            if m.mode == Mode::Compose {
                m.compose_draft.get_or_insert_with(String::new).push(c);
            }
        }
        Msg::ComposeBackspace => {
            if m.mode == Mode::Compose
                && let Some(d) = m.compose_draft.as_mut()
            {
                d.pop();
            }
        }
        Msg::ComposeEnter => {
            if m.mode == Mode::Compose {
                let body = m.compose_draft.clone().unwrap_or_default();
                let trimmed = body.trim().to_string();
                if trimmed.is_empty() {
                    m.set_status(StatusKind::Error, "post body cannot be empty");
                } else {
                    effects.push(Effect::PostBoard { body: trimmed });
                }
            }
        }
        Msg::ComposeCancel => {
            m.compose_draft = None;
            m.mode = Mode::Normal;
            m.set_status(StatusKind::Info, "compose cancelled");
        }
    }

    // Keep diagnostics selected cleanly if user jumped to Diagnostics.
    if m.inspector == Inspector::Diagnostics {
        m.clamp_selection();
    }
    (m, effects)
}

fn event_driven_loads() -> Vec<Effect> {
    vec![
        Effect::LoadBoard,
        Effect::LoadRoster,
        Effect::LoadLeases,
        Effect::LoadReceipts,
        Effect::LoadSealedCounts,
    ]
}

const fn is_user_input(msg: &Msg) -> bool {
    matches!(
        msg,
        Msg::Quit
            | Msg::ToggleHelp
            | Msg::SelectInspector(_)
            | Msg::CycleInspector(_)
            | Msg::MovePost(_)
            | Msg::GotoPost(_)
            | Msg::ScrollMessage(_)
            | Msg::ToggleFollow
            | Msg::ToggleRetired
            | Msg::MoveDiag(_)
            | Msg::CopyRemedy
            | Msg::OpenFilter
            | Msg::CancelFilter
            | Msg::FilterChar(_)
            | Msg::FilterBackspace
            | Msg::CommitFilter
            | Msg::Refresh
            | Msg::StepRefresh(_)
            | Msg::ToggleRelativeTime
            | Msg::OpenCompose
            | Msg::ComposeChar(_)
            | Msg::ComposeBackspace
            | Msg::ComposeEnter
            | Msg::ComposeCancel
    )
}
