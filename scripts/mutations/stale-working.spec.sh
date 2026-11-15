# Sourced by scripts/verify-mutations.sh.
#
# bd-0uzrq.10: a `working` program_status record can be a lie. opencode emits
# `state=working` at turn start and does not emit `idle` after a turn that ran a
# background shell, so the record sits at working while the pane is at its
# prompt — and the dispatcher never drains a queued wake.
#
#   scripts/verify-mutations.sh scripts/mutations/stale-working.spec.sh --cargo mbx
#
# Fixtures are the live captures of 2026-11-15 (records + screen for the stuck
# pane, and an idle pane for the control).
#
# What this does not prove: the live wiring (`block_state` deciding to spend a
# capture), covered by the live leg.

CARGO_ARGS=(-p toron-plane --lib)
TEST_FILTER=(stale_working idle_footer record_age tool_call_gap context_row)

REX=crates/toron-plane/src/backend/rex_cli.rs

mutation "D1  OSC alone is trusted, so a stuck working record stays working" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        if status != AgentStatus::Working || !stale || !screen_shows_a_prompt(screen) {' \
    replace='        if status != AgentStatus::Working {' 

mutation "D2  the screen overrules a FRESH working record" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        let stale = record_age_secs.is_some_and(|age| age >= STALE_WORKING_SECS);' \
    replace='        let stale = true;'

mutation "D3  the context row must start with / again (the live narrow footer)" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        let row = line.trim_end();' \
    replace='        let row = line.trim_end();
        if !row.trim_start().starts_with('"'"'/'"'"') { return false; }'

mutation "D4  the progress markers are not consulted" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        let lower = line.to_ascii_lowercase();' \
    replace='        let lower = String::new();' 

mutation "D5  a single capture counts as a stable footer" \
    file=$REX \
    expect=a_tool_call_gap_is_not_a_finished_turn \
    find='        let stable = now_unix.saturating_sub(sample.first_seen_unix) >= min_stable_secs;' \
    replace='        let stable = true;'

mutation "D6  the footer digest is ignored, so a redrawn gap looks stable" \
    file=$REX \
    expect=a_tool_call_gap_is_not_a_finished_turn \
    find='            Some(p) if p.digest == digest => p,' \
    replace='            Some(p) => p,'

mutation "D7  any elapsed time counts, so the minimum is not enforced" \
    file=$REX \
    expect=a_tool_call_gap_is_not_a_finished_turn \
    find='        let stable = now_unix.saturating_sub(sample.first_seen_unix) >= min_stable_secs;' \
    replace='        let stable = now_unix.saturating_sub(sample.first_seen_unix) >= 0;'

mutation "D8  a label-only context row (no counter) is not a prompt" \
    file=$REX \
    expect=a_context_row_without_the_counter_is_still_a_prompt \
    find='            || lines.iter().any(|line| is_label_only_context_row(line))' \
    replace='            || false'

mutation "D9  the label-only arm ignores the progress row" \
    file=$REX \
    expect=a_context_row_without_the_counter_is_still_a_prompt \
    find='        if lines.is_empty() || lines.iter().any(|line| is_progress_row(line)) {' \
    replace='        if lines.is_empty() || lines.len() == usize::MAX {'
