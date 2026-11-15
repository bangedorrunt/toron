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
TEST_FILTER=(stale_working idle_footer record_age)

REX=crates/toron-plane/src/backend/rex_cli.rs

mutation "D1  OSC alone is trusted, so a stuck working record stays working" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        if status == AgentStatus::Working' \
    replace='        if false && status == AgentStatus::Working'

mutation "D2  the screen overrules a FRESH working record" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='            && record_age_secs.is_some_and(|age| age >= STALE_WORKING_SECS)' \
    replace='            && true'

mutation "D3  the context row must start with / again (the live narrow footer)" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        let row = line.trim_end();' \
    replace='        let row = line.trim_end();
        if !row.trim_start().starts_with('"'"'/'"'"') { return false; }'

mutation "D4  the progress markers are not consulted" \
    file=$REX \
    expect=a_stale_working_record_is_overruled_by_the_idle_footer \
    find='        let lower = row.to_ascii_lowercase();' \
    replace='        let lower = String::new();'
