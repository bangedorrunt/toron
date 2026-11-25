# Sourced by scripts/verify-mutations.sh.
# bd-gs3m: pure diagnose_toron rules + 300-tick probe cadence / idle-pause bounds.

CARGO_ARGS=(-p toron-cli --bin toron)
TEST_FILTER=(
    diagnose_toron_rules_cover_wake_and_queued_sends
    fake_clock_300_ticks_bounds_toron_and_flywheel_probes
)

mutation "P0  wake deaf rule never fires" \
    file=crates/toron-cli/src/tui/model.rs \
    expect=diagnose_toron_rules_cover_wake_and_queued_sends \
    find='    } else if !signals.wake_liveness_ok {' \
    replace='    } else if false && !signals.wake_liveness_ok {'

mutation "P1  wake publish failures rule never fires" \
    file=crates/toron-cli/src/tui/model.rs \
    expect=diagnose_toron_rules_cover_wake_and_queued_sends \
    find='    if signals.wake_publish_failures > 0 {' \
    replace='    if signals.wake_publish_failures > 999 {'

mutation "P1  stuck queued sends rule never fires" \
    file=crates/toron-cli/src/tui/model.rs \
    expect=diagnose_toron_rules_cover_wake_and_queued_sends \
    find='    if signals.stuck_queued_sends > 0 {' \
    replace='    if signals.stuck_queued_sends > 999 {'

mutation "P1  probe cadence ignores idle/unfocused pause" \
    file=crates/toron-cli/src/tui/update.rs \
    expect=fake_clock_300_ticks_bounds_toron_and_flywheel_probes \
    find='            if !m.focused || now_unix.saturating_sub(m.last_input_unix) >= IDLE_PAUSE_SECS {' \
    replace='            if false && (!m.focused || now_unix.saturating_sub(m.last_input_unix) >= IDLE_PAUSE_SECS) {'
